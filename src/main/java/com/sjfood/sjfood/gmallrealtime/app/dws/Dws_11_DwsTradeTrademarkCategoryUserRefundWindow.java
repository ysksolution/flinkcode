package com.sjfood.sjfood.gmallrealtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.TradeTrademarkCategoryUserRefundBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.function.DimAsyncFunction1;
import com.sjfood.sjfood.gmallrealtime.util.AtguiguiUtil;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * @Date: 2022/11/17/14:41
 * @Package_name: com.sjfood.sjfood.gmallrealtime.app.dws
 */
public class Dws_11_DwsTradeTrademarkCategoryUserRefundWindow extends BaseAPPV1 {

    public static void main(String[] args) {

        new Dws_11_DwsTradeTrademarkCategoryUserRefundWindow().init(
                4011,2,"Dws_11_DwsTradeTrademarkCategoryUserRefundWindow", Constant.DWD_TRADE_ORDER_REFUND
        );

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1.封装为 Pojo
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> pojoStream = parseToPojo(stream);

        //2.补充维度信息
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> dim1Stream = joinDim(pojoStream);


        //3.开窗聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> joinSteam = windowAndJoin(dim1Stream);

        //4.补充无关维度信息
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultStream = joinDim2(joinSteam);
//        resultStream.print();

        //4.写出到 clickhouse
        resultStream.addSink(FlinkSinKUtil.getClickHouseSink("dws_trade_trademark_category_user_refund_window",
                TradeTrademarkCategoryUserRefundBean.class));


    }

    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> joinDim2(SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> joinSteam) {

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> stream1 = AsyncDataStream.unorderedWait(
                joinSteam,
                new DimAsyncFunction1<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    protected String getTable() {
                        return "dim_base_category1";
                    }

                    @Override
                    protected String getId(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    protected void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dims) {
                        input.setCategory1Name(dims.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

       return AsyncDataStream.unorderedWait(
                stream1,
                new DimAsyncFunction1<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    protected String getTable() {
                        return "dim_base_trademark";
                    }

                    @Override
                    protected String getId(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    protected void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dims) {
                        input.setTrademarkName(dims.getString(" TM_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );



    }

    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> windowAndJoin(SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> dim1Stream) {

        return dim1Stream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj,ts) -> obj.getTs())
                                .withIdleness(Duration.ofSeconds(20))
                )
                .keyBy( bean -> bean.getCategory3Id() +
                        "_" + bean.getCategory2Id() +
                        "_" + bean.getCategory1Id() +
                        "_" + bean.getTrademarkId() +
                        "_" + bean.getUserId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {

                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {

                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                        new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<TradeTrademarkCategoryUserRefundBean> elements, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {

                        TradeTrademarkCategoryUserRefundBean bean = elements.iterator().next();
                        bean.setRefundCount((long) bean.getOrderIdSet().size());
                        bean.setStt(AtguiguiUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguiUtil.tsToDateTime(ctx.window().getEnd()));
                        bean.setTs(System.currentTimeMillis());
                        out.collect(bean);

                    }
                });
    }

    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> joinDim(SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> pojoStream) {
        /**
         *   DataStream<IN> in,
         *  AsyncFunction<IN, OUT> func,
         *  long timeout,
         *  TimeUnit timeUnit,
         */
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> skuInfoStream = AsyncDataStream.unorderedWait(
                pojoStream,
                new DimAsyncFunction1<TradeTrademarkCategoryUserRefundBean>() {

                    @Override
                    protected String getTable() {
                        return "dim_sku_info";
                    }

                    @Override
                    protected String getId(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    protected void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dims) {

                        input.setCategory3Id(dims.getString("CATEGORY3_ID"));
                        input.setTrademarkId(dims.getString("TM_ID"));

                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c3Stream = AsyncDataStream.unorderedWait(
                skuInfoStream,
                new DimAsyncFunction1<TradeTrademarkCategoryUserRefundBean>() {

                    @Override
                    protected String getTable() {
                        return "dim_base_category3";
                    }

                    @Override
                    protected String getId(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    protected void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dims) {

                        input.setCategory2Id(dims.getString("CATEGORY2_ID"));
                        input.setCategory3Name(dims.getString("NAME"));

                    }
                },
                60,
                TimeUnit.SECONDS
        );

       return AsyncDataStream.unorderedWait(
               c3Stream,
                new DimAsyncFunction1<TradeTrademarkCategoryUserRefundBean>() {

                    @Override
                    protected String getTable() {
                        return "dim_base_category2";
                    }

                    @Override
                    protected String getId(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    protected void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dims) {

                        input.setCategory1Id(dims.getString("CATEGORY1_ID"));
                        input.setCategory2Name(dims.getString("NAME"));

                    }
                },
                60,
                TimeUnit.SECONDS
        );




    }

    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> parseToPojo(DataStreamSource<String> stream) {

        return stream
                .map(new MapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean map(String value) throws Exception {

                        JSONObject obj = JSON.parseObject(value);
                        return  TradeTrademarkCategoryUserRefundBean.builder()
                                .userId(obj.getString("user_id"))
                                .ts(obj.getLong("ts") * 1000)
                                .skuId(obj.getString("sku_id"))
                                .orderIdSet(new HashSet<>(Collections.singleton(obj.getString("order_id"))))
                                .build();

                    }
                });


    }
}
