package com.sjfood.sjfood.gmallrealtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.TrafficPageViewBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


@Slf4j
public class Copy_Dws_02_DwsTrafficVcChArIsNewPageViewWindow extends BaseAPPV1 {

    public static void main(String[] args) {

        new Copy_Dws_02_DwsTrafficVcChArIsNewPageViewWindow().init(
                4002, 2, "Dws_02_DwsTrafficVcChArIsNewPageViewWindow", Constant.DWD_TRAFFIC_PAGE
        );

    }

    /*
        10.2 流量域 版本-渠道-地区-访客类别 粒度页面浏览各窗口汇总表
        統計统计每个窗口内：页面浏览数、浏览总时长、独立访客数、跳出会话数
        1、页面浏览数，每个页面算 1 个
        2、获取每个页面的浏览时长
        3、每日独立访客数
            设置一个单值状态，如果该状态为null或者不为今天
        4、跳出会话数，last_page_id is null，代表是一个新会话

        本节的任务是统计这五个指标，并将维度和度量数据写入 ClickHouse 汇总表。
     */

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1.数据清洗
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        etledStream.print();

//        //2.把数据封装到一个 pojo 中
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = parseToPojo(etledStream);
//
//        //3.开窗聚合
//        SingleOutputStreamOperator<TrafficPageViewBean> resultStream = windowAndAgg(beanStream);
//
//        //4.写出到clickhouse中
//
//        writeToClickhouse(resultStream);


    }

    private SingleOutputStreamOperator<TrafficPageViewBean> parseToPojo(SingleOutputStreamOperator<JSONObject> etledStream) {

        etledStream.keyBy(value -> value.getJSONObject("common").getString(""));
        return null;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {

        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                try{
                    JSONObject.parseObject(value);
                }catch (Exception e){
                    log.warn(value+"不是合格的json數據");
                }
                return true;
            }

            })
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
//                        System.out.println("value:"+value);
                        return JSONObject.parseObject(value);
                    }
                });
        }
}

//    private void writeToClickhouse(SingleOutputStreamOperator<TrafficPageViewBean> resultStream) {
//
//        resultStream.addSink(FlinkSinKUtil.getClickHouseSink(" dws_traffic_vc_ch_ar_is_new_page_view_window ",TrafficPageViewBean.class));
//
//    }
//
//    /**
//     * 窗口处理函数
//     *  增量(来一条计算一条)：
//     *      简单：max,min,sum,maxBy,minBy
//     *      复杂：
//     *          reduce：输入和输出类型一致 （来一条处理一条）
//     *          aggregate:输入和输出的类型可以不一致累加器
//     *  全量（将数据全部拿过来，想怎么处理就怎么处理）：
//     *
//     * @return
//     */
//    private SingleOutputStreamOperator<TrafficPageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficPageViewBean> beanStream) {
//
//       return beanStream
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                                .withTimestampAssigner((bean ,ts) -> bean.getTs())
//                                //这个可以避免由于数据倾斜导致的水印不更新问题
//                                .withIdleness(Duration.ofSeconds(20))
//
//                )
//                //keyby 的 key 永远是用字符串
//                .keyBy(bean -> bean.getAr() + "_" + bean.getCh() + "_" + bean.getIsNew() + "_" + bean.getVc())
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .reduce(
//                        new ReduceFunction<TrafficPageViewBean>() {
//
//                            @Override
//                            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
//
//                                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
//                                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
//                                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
//                                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
//
//                                return value1;
//                            }
//
//                        }
//                        ,
//                        new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
//                            @Override
//                            public void process(String key,
//                                                Context ctx,
//                                                //elements 存储的是前面聚合的最终结果：有且仅有一个值
//                                                Iterable<TrafficPageViewBean> elements,
//                                                Collector<TrafficPageViewBean> out) throws Exception {
//
//                                TrafficPageViewBean bean = elements.iterator().next();
//
//                                bean.setStt(AtguiguiUtil.tsToDateTime(ctx.window().getStart()));
//                                bean.setEdt(AtguiguiUtil.tsToDateTime(ctx.window().getEnd()));
//
//                                //把 ts 改成统计时间
//                                bean.setTs(System.currentTimeMillis());
//                                out.collect(bean);
//
//                            }
//                        }
//
//                );
////                .print();
//
//    }
//
//    private SingleOutputStreamOperator<TrafficPageViewBean> parseToPojo(SingleOutputStreamOperator<String> stream) {
//
//       return stream
//                .map(JSON::parseObject)
//                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
//                .map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {
//
//
//                    private ValueState<String> visitDateState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//
//                        visitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visitDateState", String.class));
//
//                    }
//
//                    @Override
//                    public TrafficPageViewBean map(JSONObject obj) throws Exception {
//
//                        JSONObject common = obj.getJSONObject("common");
//                        JSONObject page = obj.getJSONObject("page");
//                        Long ts = obj.getLong("ts");
//
//                        String vc = common.getString("vc");
//                        String ch = common.getString("ch");
//                        String ar = common.getString("ar");
//                        String isNew = common.getString("is_new");
//
//                        //是否是当天当前mid的第一条记录
//                        long uvCt = 0L;
//                        //这条数据的年月日与状态是否一致，一致就是当天的第一条，否则就不是第一条
//                        String today = AtguiguiUtil.tsToDate(ts);
//
//                        String visitDate = visitDateState.value();
//
//                        //把常量，一定有值的数据放在前面
//
//                        /*
//                            由于数据积压，昨天的数据也出现了
//                            30号有两条数据，31号有三条数据进来
//                            A、B、C、D、E
//                            当A进来，
//
//                         */
//                        if (!today.equals(visitDate)) {
//                            uvCt = 1L;
//                            visitDateState.update(today);
//                        }
//
//
//                        long svCt = 0L;
//
//                        String lastPageId = page.getString("last_page_id");
//                        if (Strings.isEmpty(lastPageId)) {
//                            svCt = 1L;
//                        }
//
//
//                        Long pvCt = 1L;
//                        Long durSum = page.getLong("during_time");
//
//
//                        return new TrafficPageViewBean(
//                                "",
//                                "",
//                                vc, ch, ar, isNew,
//                                uvCt, svCt, pvCt, durSum,
//                                ts
//
//                        );
//                    }
//                });
//
//
//
//    }
//
//    private SingleOutputStreamOperator<String> etl(DataStreamSource<String> stream) {
//
//        return stream
//                .filter(new FilterFunction<String>() {
//                    @Override
//                    public boolean filter(String value) throws Exception {
//
//                        try {
//                            JSON.parseObject(value);
//                        } catch (Exception e) {
//                            log.warn("数据格式不是合法的："+value);
//                            return false;
//                        }
//
//                        return true;
//
//                    }
//
//                });
//    }
//}
///*
//    会话数、页面浏览数、浏览总时长和独立访客三个指标均与页面浏览有关，可以由 DWD 层页面浏览明细表获得。
//1）读取页面主题数据，封装为流
//2）转换数据结构，按照设备id分组
//3）统计页面浏览时长、页面浏览数、会话数和独立访客数，转换数据结构
//
//    创建实体类，将页面浏览数置为 1（只要有一条页面浏览日志，则页面浏览数加一），获取日志中的页面浏览时长，赋值给实体类的同名字段。
//    接下来判断 last_page_id 是否为 null，如果是，说明页面是首页，开启了一个新的会话，将会话数置为 1，否则置为 0。
//
//    每日独立访客数量，状态保存24h后就会被清除，所以用户每天的首日访问日期都是null
//    最后在状态中维护末次访问日期，如果状态中的日期为null或不为当日，则将独立访客数置为1，否则置为0。（没问题）
//    如果访问日期是null，说明之前没有访问过，该用户是首次访问，这个没有问题
//    如果访问日期是今天，说明今天已经访问过了，置0
//    不用去考虑历史数据，只考虑实时数据
//
//   select
//    visitDate,
//    count(distinct mid) cnt
//   from test
//   group by visitDate,mid
//
//
//
//    独立访客每日重置，所以状态最多维护一天即可，为了减少内存开销，设置状态ttl为1天。
//
//补充维度字段，窗口起始和结束时间置为空字符串。下游要根据水位线开窗，所以要补充事件时间字段，此处将日志生成时间 ts 作为事件时间字段即可。最后将实体类对象发往下游。
//4）设置水位线
//5）按照维度字段分组
//6）开窗
//	跳出行为判定的超时时间为 10s，假设某条日志属于跳出数据，如果它对应的事件时间为 15s，要判定是否跳出需要在水位线达到 25s 时才能做到，若窗口大小为 10s，这条数据应进入 10~20s 窗口，但是拿到这条数据时水位线已达到 25s，所属窗口已被销毁。这样就导致跳出会话数永远为 0，显然是有问题的。要避免这种情况，必须设置窗口延迟关闭，延迟关闭时间大于等于跳出判定的超时时间才能保证跳出数据不会被漏掉。但是这样会严重影响时效性，如果企业要求延迟时间设置为半小时，那么窗口就要延迟半小时关闭。要统计跳出行为相关的指标，就必须接受它对时效性带来的负面影响。
//7）聚合计算
//度量字段求和，每个窗口数据聚合完毕之后补充窗口起始时间和结束时间字段。
//在 ClickHouse 中，ts 将作为版本字段用于去重，ReplacingMergeTree 会在分区合并时对比排序字段相同数据的 ts，保留 ts 最大的数据。此处将时间戳字段置为当前系统时间，这样可以保证数据重复计算时保留的是最后一次计算的结果。
//
// */