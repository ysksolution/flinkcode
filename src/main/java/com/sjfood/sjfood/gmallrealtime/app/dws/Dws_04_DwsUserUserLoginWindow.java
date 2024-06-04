package com.sjfood.sjfood.gmallrealtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.UserLoginBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.AtguiguiUtil;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/14/19:46
 * @Package_name: com.sjfood.sjfood.gmallrealtime.app.dws
 */

/*
数据源：
    日志数据：页面日志
1.过滤出登陆记录

//自动登陆
//入口的页面的时候，自动完成登陆
//last_page_id == null && uid != null
//在访问的过程中由于需要会跳转到登陆页面，然后登陆，再跳转到先前的页面中
// a -> login -> a (需要这个页面)
//last_page_id == login && uid != null


2.解析成pojo

3.开窗聚合

4.写出到 clickhouse 中


登陆用户：
    当日独立用户数
    七日回流用户
当日独立用户数

七日回流用户
    定义一个状态，保存的是最后一次登陆日期

    判断这次登陆日期与最后一次登陆的日期的差值： >7 就是 7 日回流用户
 */
public class Dws_04_DwsUserUserLoginWindow extends BaseAPPV1 {

    public static void main(String[] args) {

        new Dws_04_DwsUserUserLoginWindow().init(
                4004, 2, "Dws_04_DwsUserUserLoginWindow", Constant.DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //1.过滤登陆记录
        SingleOutputStreamOperator<JSONObject> filteredStream = filterLoginLog(stream);
//        filteredStream.print();

        //2.解析成pojo
        SingleOutputStreamOperator<UserLoginBean> beanStream = parseToPojo(filteredStream);

        //3.开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultStream = windowAndAgg(beanStream);

        //4.写到clickhouse中
        writeToClickHouse(resultStream);


    }

    private void writeToClickHouse(SingleOutputStreamOperator<UserLoginBean> resultStream) {
        resultStream.addSink(FlinkSinKUtil.getClickHouseSink("dws_user_user_login_window",UserLoginBean.class));
    }

    private SingleOutputStreamOperator<UserLoginBean> windowAndAgg(SingleOutputStreamOperator<UserLoginBean> beanStream) {

        return beanStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<UserLoginBean, Tuple2<Long, Long>, UserLoginBean>() {
                               @Override
                               public Tuple2<Long, Long> createAccumulator() {
                                   return Tuple2.of(0L, 0L);
                               }

                               @Override
                               public Tuple2<Long, Long> add(UserLoginBean bean, Tuple2<Long, Long> acc) {
                                   return Tuple2.of(acc.f0 + bean.getBackCt(), acc.f1 + bean.getUuCt());
                               }

                               @Override
                               public UserLoginBean getResult(Tuple2<Long, Long> accumulator) {
                                   return new UserLoginBean("", "", accumulator.f0, accumulator.f1, 0L);
                               }

                               //当窗口是session的时候，窗口才会触发

                               @Override
                               public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                                   return null;
                               }
                           },
                            new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {

                                @Override
                                public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                                    UserLoginBean bean = values.iterator().next();

                                    bean.setStt(AtguiguiUtil.tsToDateTime(window.getStart()));
                                    bean.setEdt(AtguiguiUtil.tsToDateTime(window.getEnd()));

                                    bean.setTs(System.currentTimeMillis());

                                    out.collect(bean);
                                }
                        }
                );
    }


    private SingleOutputStreamOperator<UserLoginBean> parseToPojo(SingleOutputStreamOperator<JSONObject> filteredStream) {

        return filteredStream
                .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        lastLoginDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDateState", String.class));

                    }

                    @Override
                    public void processElement(JSONObject obj , Context ctx, Collector<UserLoginBean> out) throws Exception {

                        //根据当前用户的今天的首次登陆记录
                        Long ts = obj.getLong("ts");
                        String today = AtguiguiUtil.tsToDate(ts);
                        String lastLoginDate = lastLoginDateState.value();

                        Long uuCt = 0L;
                        Long backCt = 0L;

                        if (!today.equals(lastLoginDate)) {
                            //这个用户的今天第一次登陆
                            uuCt = 1L;
                            lastLoginDateState.update(today);

                            //这个用户今天的首次登陆，有可能是回流用户
                            if (lastLoginDate != null) {//如果不是首次登陆，则才需要判断是否回流
                                Long lastLoginTs = AtguiguiUtil.dataToTs(lastLoginDate);

                                if ((ts - lastLoginTs) / 1000 / 60 / 60 / 24  > 7){

                                    backCt = 1L;

                                }
                            }

                        }

                        if (uuCt == 1){
                            out.collect(new UserLoginBean("","",backCt,uuCt,ts));
                        }

                    }
                });



    }

    private SingleOutputStreamOperator<JSONObject> filterLoginLog(DataStreamSource<String> stream) {



        return stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean,ts) -> bean.getLong("ts"))
                )
                .filter(obj -> {
                    String uid = obj.getJSONObject("common").getString("uid");
                    String lastPageId = obj.getJSONObject("page").getString("last_page_id");

                    return ((lastPageId == null || lastPageId.equals("login")) && uid != null);

                });


    }


}
