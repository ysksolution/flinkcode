package com.sjfood.sjfood.gmallrealtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.UserRegisterBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.AtguiguiUtil;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * @Date: 2022/11/14/21:12
 * @Package_name: com.sjfood.sjfood.gmallrealtime.app.dws
 */
public class Dws_05_DwsUserRegisterWindow extends BaseAPPV1 {

    public static void main(String[] args) {

        new Dws_05_DwsUserRegisterWindow()
                .init(
                        4005, 2, "Dws_05_DwsUserRegisterWindow", Constant.DWD_USER_REGISTER
                );

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        stream
                .map(str -> {

                    JSONObject obj = JSON.parseObject(str);

                    return new UserRegisterBean("", "", 1L, obj.getLong("create_time"));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(20))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(

                        new ReduceFunction<UserRegisterBean>() {

                            @Override
                            public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {

                                value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<UserRegisterBean> elements, Collector<UserRegisterBean> out) throws Exception {

                                UserRegisterBean bean = elements.iterator().next();

                                bean.setStt(AtguiguiUtil.tsToDateTime(context.window().getStart()));
                                bean.setEdt(AtguiguiUtil.tsToDateTime(context.window().getEnd()));

                                bean.setTs(System.currentTimeMillis());

                                out.collect(bean);


                            }
                        }
                )
//                .print();
                .addSink(FlinkSinKUtil.getClickHouseSink("dws_user_user_register_window",UserRegisterBean.class));

    }
}
