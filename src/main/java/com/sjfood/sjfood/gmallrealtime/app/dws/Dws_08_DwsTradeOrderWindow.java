package com.sjfood.sjfood.gmallrealtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.TradeOrderBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.AtguiguiUtil;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/15/15:00
 * @Package_name: com.sjfood.sjfood.gmallrealtime.app.dws
 */
public class Dws_08_DwsTradeOrderWindow extends BaseAPPV1 {
    public static void main(String[] args) {
        new Dws_08_DwsTradeOrderWindow().init(
                4008,2,"Dws_08_DwsTradeOrderWindow", Constant.DWD_TRADE_ORDER_DETAIL
        );
    }

    /**
     *
         统计当日下单独立用户数和新增下单用户数
        当日下单独立用户数：状态存活设置为一天

        #当日下单独立用户数
        select
            cur_date,
            count(distinct mid) cnt
        from table
        group by cur_date,mid

        下单新用户数
        第一次下单:获取首日访问日期
     select
            count(1)
     from (
        select
            cur_date,
            mid,
       row number(group by mid order by cur_date) rn
        from table
     )t1
     where rn = 1
     */

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj,ts)->obj.getLong("ts")*1000)
                )
                .keyBy(obj -> obj.getString("user_id"))
                //封装为Pojo
               .flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {

                   private ValueState<String> orderDataState;

                   @Override
                   public void open(Configuration parameters) throws Exception {

                       orderDataState = getRuntimeContext().getState(new ValueStateDescriptor<String>("orderDataState", String.class));
                   }

                   @Override
                   public void flatMap(JSONObject obj, Collector<TradeOrderBean> out) throws Exception {

                       Long ts = obj.getLong("ts") * 1000;

                       String today = AtguiguiUtil.tsToDate(ts);

                       String orderDate = orderDataState.value();

                       // 下单独立用户数
                       long orderUniqueUserCount = 0L;

                       // 下单新用户数
                       long orderNewUserCount = 0L;

                       if (!today.equals(orderDate)) {

                           //独立用户
                           orderUniqueUserCount = 1L;
                           //更新状态
                           orderDataState.update(today);

                           if ( orderDate == null ) {
                               orderNewUserCount = 1L;
                           }
                       }
//                       out.collect(new TradeOrderBean("", "", orderUniqueUserCount, orderNewUserCount, ts));
                       if (orderUniqueUserCount == 1) {
                           out.collect(new TradeOrderBean("", "", orderUniqueUserCount, orderNewUserCount, ts));
                       }
                   }
               })
//                .print();
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                            @Override
                            public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {

                                value1.setOrderNewUserCount(value1.getOrderNewUserCount()+value2.getOrderNewUserCount());
                                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount()+value2.getOrderUniqueUserCount());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx, Iterable<TradeOrderBean> elements, Collector<TradeOrderBean> out) throws Exception {

                                TradeOrderBean bean = elements.iterator().next();

                                bean.setStt(AtguiguiUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(AtguiguiUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setTs(System.currentTimeMillis());
                                out.collect(bean);
                            }
                        })
                .addSink(FlinkSinKUtil.getClickHouseSink("dws_trade_order_window",TradeOrderBean.class));
    }
}
