package com.sjfood.sjfood.gmallrealtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.TradePaymentWindowBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.AtguiguiUtil;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/15/10:23
 * @Package_name: com.sjfood.sjfood.gmallrealtime.app.dws
 */
public class Dws_07_DwsTradePaymentSucWindow extends BaseAPPV1 {

    public static void main(String[] args) {

        new Dws_07_DwsTradePaymentSucWindow().init(
                4007,2,"Dws_07_DwsTradePaymentSucWindow", Constant.DWD_TRADE_PAY_DETAIL_SUC
        );

    }

/*
 支付成功
 支付成功独立用户数和首次支付成功用户数。
 支付成功独立用户数：
    每日：
        state 和 value不一致，每日
 首次支付成功用户数：
    state == null &&  state 和 value不一致

*/
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1.过滤出
        stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj,ts) -> obj.getLong("ts") * 1000)
                                .withIdleness(Duration.ofSeconds(20))

                )
                .keyBy(value -> value.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>() {


                    private ValueState<String> dataState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        dataState = getRuntimeContext().getState(new ValueStateDescriptor<String>("dataState", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj, Context ctx, Collector<TradePaymentWindowBean> out) throws Exception {

                        long ts = obj.getLong("ts") * 1000;
                        String today = AtguiguiUtil.tsToDate(ts);

                        Long paymentSucUniqueUserCount = 0L;
                        Long paymentSucNewUserCount = 0L;

                        String lastDateState = dataState.value();

                        if (!today.equals(lastDateState)) {

                            //独立用户
                            paymentSucUniqueUserCount = 1L;
                            //更新状态
                            dataState.update(today);

                            if (lastDateState == null) {
                                paymentSucNewUserCount = 1L;
                            }

                        }

                        if (paymentSucUniqueUserCount == 1) {

                            out.collect(new TradePaymentWindowBean("","",paymentSucUniqueUserCount,paymentSucNewUserCount,ts));
                        }

                    }
                })
         .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
          .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                      @Override
                      public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {

                          value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount()+value2.getPaymentSucNewUserCount());
                          value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount()+value2.getPaymentSucUniqueUserCount());
                          return value1;
                      }
                  },
                  new ProcessAllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                      @Override
                      public void process(Context ctx, Iterable<TradePaymentWindowBean> elements, Collector<TradePaymentWindowBean> out) throws Exception {


                          TradePaymentWindowBean bean = elements.iterator().next();

                          bean.setStt(AtguiguiUtil.tsToDateTime(ctx.window().getStart()));
                          bean.setEdt(AtguiguiUtil.tsToDateTime(ctx.window().getEnd()));

                          bean.setTs(System.currentTimeMillis());

                          out.collect(bean);



                      }
                  })
                .addSink(FlinkSinKUtil.getClickHouseSink("dws_trade_payment_suc_window",TradePaymentWindowBean.class));




    }
}
