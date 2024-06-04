package com.sjfood.sjfood.gmallrealtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;

import com.sjfood.sjfood.gmallrealtime.bean.TrafficHomeDetailPageViewBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;

import com.sjfood.sjfood.gmallrealtime.util.AtguiguiUtil;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
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
 * @Date: 2022/11/14/19:04
 * @Package_name: com.sjfood.sjfood.gmallrealtime.app.dws
 */

/**
 * 统计当日的首页和商品详情页的独立访客数
 * 1.只过滤首页和详情页数据
 * 2.解析成Pojo类型
 * 3.开窗聚合
 * 4.写出到 clickhouse 中
 */
public class Dws_03_DwsTrafficPageViewWindow extends BaseAPPV1 {


    public static void main(String[] args) {

        new Dws_03_DwsTrafficPageViewWindow().init(
                4003,2,"Dws_03_DwsTrafficPageViewWindow", Constant.DWD_TRAFFIC_PAGE
        );

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1.只过滤首页和详情页数据
        stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj,ts) -> obj.getLong("ts"))

                )
                .filter( obj -> {
                    String pageId = obj.getJSONObject("page").getString("page_id");
                    return "home".equals(pageId) || "good_detail".equals(pageId);
                })
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
               .flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

                   private ValueState<String> visitGoodDetailDataState;
                   private ValueState<String> visitHomeDataState;

                   @Override
                   public void open(Configuration parameters) throws Exception {
                       visitHomeDataState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visitHomeDataState", String.class));
                       visitGoodDetailDataState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visitGoodDetailDataState", String.class));
                   }

                   @Override
                   public void flatMap(JSONObject obj, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                       Long ts = obj.getLong("ts");
                       String pageId = obj.getJSONObject("page").getString("page_id");

                       Long homeUvCt = 0L;
                       Long goodDetailUvCt = 0L;

                       String today = AtguiguiUtil.tsToDate(ts);

                       if ("home".equals(pageId) && !today.equals(visitHomeDataState.value())) {
                           homeUvCt = 1L;
                           visitHomeDataState.update(today);
                       }

                       if ("good_detail".equals(pageId) && !today.equals(visitGoodDetailDataState.value())) {
                           goodDetailUvCt = 1L;
                           visitGoodDetailDataState.update(today);
                       }

                       TrafficHomeDetailPageViewBean bean = new TrafficHomeDetailPageViewBean(
                               "", "",
                               homeUvCt, goodDetailUvCt, ts
                       );

                       if (homeUvCt + goodDetailUvCt == 1) {
                           out.collect(bean);
                       }


                   }
               })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {

                                value1.setHomeUvCt(value1.getHomeUvCt()+value2.getHomeUvCt());
                                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt()+value2.getGoodDetailUvCt());

                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<TrafficHomeDetailPageViewBean> elements,
                                                Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                                TrafficHomeDetailPageViewBean bean = elements.iterator().next();
                                bean.setStt(AtguiguiUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(AtguiguiUtil.tsToDateTime(ctx.window().getEnd()));

                                bean.setTs(System.currentTimeMillis());

                                out.collect(bean);

                            }
                        }

                )
        .addSink(FlinkSinKUtil.getClickHouseSink("dws_traffic_page_view_window",TrafficHomeDetailPageViewBean.class));

    }
}
