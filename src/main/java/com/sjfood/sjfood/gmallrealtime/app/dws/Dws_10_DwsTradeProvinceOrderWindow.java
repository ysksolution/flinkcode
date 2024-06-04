package com.sjfood.sjfood.gmallrealtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.TradeProvinceOrderWindow;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.function.DimAsyncFunction1;
import com.sjfood.sjfood.gmallrealtime.util.AtguiguiUtil;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/16/16:35
 * @Package_name: com.sjfood.sjfood.gmallrealtime.app.dws
 */
public class Dws_10_DwsTradeProvinceOrderWindow extends BaseAPPV1 {

    public static void main(String[] args) {

        new Dws_10_DwsTradeProvinceOrderWindow().init(
                6001,2,"Dws_10_DwsTradeProvinceOrderWindow", Constant.DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1.封装为pojo
        SingleOutputStreamOperator<TradeProvinceOrderWindow> pojoStream = parseToPojo(stream);

        //2.去重
        SingleOutputStreamOperator<TradeProvinceOrderWindow> distinctedStream = distinctByOrderDetailId(pojoStream);

        //3.开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStreamWithoutDim = joinWindow(distinctedStream);
//        joindedStream.print();

        //4.和维度数据进行join
        SingleOutputStreamOperator<TradeProvinceOrderWindow> resultStream = joinDim(beanStreamWithoutDim);
        resultStream.print();

        //5.写出到 clickhouse 中
        resultStream.addSink(FlinkSinKUtil.getClickHouseSink("dws_trade_province_order_window",TradeProvinceOrderWindow.class));


    }


    /**  join 的思想：
     *  来一条数据，就进行join
     *
     * @return
     */
    private SingleOutputStreamOperator<TradeProvinceOrderWindow> joinDim(SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStreamWithoutDim) {

        /**
         *   public static <IN, OUT> SingleOutputStreamOperator<OUT> unorderedWait(
         *             DataStream<IN> in,
         *             AsyncFunction<IN, OUT> func,
         *             long timeout,
         *             TimeUnit timeUnit,
         *             int capacity) {
         */
        //有序等待：谁先发送请求，就得在处理完它之后，再处理后面得的其他数据
        return AsyncDataStream.unorderedWait(

                beanStreamWithoutDim,

                new DimAsyncFunction1<TradeProvinceOrderWindow>() {
                    @Override
                    protected String getTable() {
                        return "dim_base_province";
                    }

                    @Override
                    protected String getId(TradeProvinceOrderWindow input) {
                        return input.getProvinceId();
                    }

                    @Override
                    protected void addDim(TradeProvinceOrderWindow input, JSONObject dims) {

                        input.setProvinceName(dims.getString("NAME"));

                    }
                },

                60,//超时时间，超过这个时间，这条数据还没有处理完，就不处理这条数据，直接处理下条数据

                TimeUnit.SECONDS
        );

    }


    private SingleOutputStreamOperator<TradeProvinceOrderWindow> joinWindow(SingleOutputStreamOperator<TradeProvinceOrderWindow> distinctedStream) {


        return distinctedStream
            .keyBy(TradeProvinceOrderWindow::getProvinceId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(new ReduceFunction<TradeProvinceOrderWindow>() {

                        @Override
                        public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {

                            value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                            value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                            return value1;
                        }
                    },
                    new ProcessWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {

                        @Override
                        public void process(String s, Context ctx, Iterable<TradeProvinceOrderWindow> elements, Collector<TradeProvinceOrderWindow> out) throws Exception {

                            TradeProvinceOrderWindow bean = elements.iterator().next();
                            bean.setOrderCount((long) bean.getOrderIdSet().size());
                            bean.setStt(AtguiguiUtil.tsToDateTime(ctx.window().getStart()));
                            bean.setEdt(AtguiguiUtil.tsToDateTime(ctx.window().getEnd()));
                            bean.setTs(System.currentTimeMillis());

                            out.collect(bean);

                        }
                    });

    }

    /**
     *去重思想：
     * 每来一条数据，就处理一条数据：
     * 用一个状态来保存上一条数据
     * 如果上一条数据是null，表名是第一条数据，那么：
     * 将这条数据写入流中，并更新状态
     * 否则，取出上一条数据，取反，将取反后的数据写入流中，
     * 把当前数据写入流中，并更新状态
     *
     *
     * @return
     */
    private SingleOutputStreamOperator<TradeProvinceOrderWindow> distinctByOrderDetailId(SingleOutputStreamOperator<TradeProvinceOrderWindow> pojoStream) {

       return  pojoStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj,ts) -> obj.getTs())
                                .withIdleness(Duration.ofSeconds(20))
                )
                .keyBy(TradeProvinceOrderWindow::getOrderDetailId)
                .flatMap(new RichFlatMapFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow>() {

                    private ValueState<TradeProvinceOrderWindow> lastBeanState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        lastBeanState = getRuntimeContext().getState(new ValueStateDescriptor<TradeProvinceOrderWindow>("lastBeanState", TradeProvinceOrderWindow.class));
                    }

                    @Override
                    public void flatMap(TradeProvinceOrderWindow value, Collector<TradeProvinceOrderWindow> out) throws Exception {

                        TradeProvinceOrderWindow lastBeanValue = lastBeanState.value();
                        if (lastBeanValue != null) {
                            lastBeanValue.setOrderAmount(lastBeanValue.getOrderAmount().negate());
                            out.collect(lastBeanValue);
                        }
                        out.collect(value);
                        lastBeanState.update(value);

                    }
                });



    }

    private SingleOutputStreamOperator<TradeProvinceOrderWindow> parseToPojo(DataStreamSource<String> stream) {
       return  stream
                .map(new MapFunction<String, TradeProvinceOrderWindow>() {

                    @Override
                    public TradeProvinceOrderWindow map(String value) throws Exception {

                        //1.解析成json
                        JSONObject obj = JSON.parseObject(value);

                        return TradeProvinceOrderWindow.builder()

                                //singleton(T) 该方法用于返回一个不可变集，只包含指定对象
                                .orderIdSet(new HashSet<>(Collections.singleton(obj.getString("order_id"))))
                                .orderDetailId(obj.getString("id"))
                                .provinceId(obj.getString("province_id"))
                                .orderAmount(obj.getBigDecimal("split_total_amount"))
                                .ts(obj.getLong("ts") * 1000)
                                .build();
                    }
                });
    }


}

/**
 *省份粒度 下单统计
 * 1.数据源：   下单事务事实表
 *
 * 2.去重：    也就是按照订单详情id
 *
 * 3.keyBy：按照省份id  开窗聚合
 *   order_amount
 *
 *   order_count
 *
     详情id   订单id    set集合   下单数
       1        1       set(1)      1
       2        1       set(1)      1
*      3        2       set(1,2)    2
 *     4        3       set(1,2,3)  3
 *
 *      set(1,2,3) -> 订单数 ：3
 *
 *      需要添加一个字段：
 *          set集合，用来存储order_id ，实现统计数量
 *
 *  4.写出到clickhouse中
 *
 */

