package com.sjfood.sjfood.gmallrealtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.TradeSkuOrderBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.function.DimAsyncFunction;
import com.sjfood.sjfood.gmallrealtime.util.AtguiguiUtil;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/15/15:52
 * @Package_name: com.sjfood.sjfood.gmallrealtime.app.dws
 */

/**
 * 从 Kafka 订单明细主题读取数据，过滤null数据，按照唯一键去重，
 * 分组开窗聚合，统计各维度各窗口的原始金额、活动减免金额、优惠券减免金额和订单金额，
 * 补全维度信息，将数据写入 ClickHouse 交易域SKU粒度下单各窗口汇总表。
 */
public class Dws_09_DwsTradeSkuOrderWindow_Cache_Async extends BaseAPPV1 {

    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow_Cache_Async().init(
                4009, 2, "Dws_09_DwsTradeSkuOrderWindow", Constant.DWD_TRADE_ORDER_DETAIL
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1.封装数据到pojo中
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(stream);

        //2.按照order_detail_id去重
        SingleOutputStreamOperator<TradeSkuOrderBean> distinctedStream = distinctByOrderId(beanStream);

        //3.开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> join = windowAndJoin(distinctedStream);

        //4.补充维度信息
        SingleOutputStreamOperator<TradeSkuOrderBean> resultStream = joinDim(join);
        resultStream.print();

        //5.写出到 clickhouse 中
        writeToClickhouse(resultStream);

    }

    private void writeToClickhouse(SingleOutputStreamOperator<TradeSkuOrderBean> resultStream) {

        resultStream.addSink(FlinkSinKUtil.getClickHouseSink("dws_trade_sku_order_window",TradeSkuOrderBean.class));

    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> joinDim(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {

        /*
                public static <IN, OUT> SingleOutputStreamOperator<OUT> unorderedWait(DataStream<IN> in, AsyncFunction<IN, OUT> func, long timeout, TimeUnit timeUnit) {}





         */
        SingleOutputStreamOperator<TradeSkuOrderBean> skuInfo = AsyncDataStream.unorderedWait(
                stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {

                    @Override
                    protected String getTable() {
                        return "dim_sku_info";
                    }

                    @Override
                    protected String getTId(TradeSkuOrderBean input) {
                        return input.getSkuId();
                    }


                    @Override
                    protected void addDim(TradeSkuOrderBean bean, JSONObject skuInfo) {
                        bean.setSkuName(skuInfo.getString("SKU_NAME"));
                        bean.setSpuId(skuInfo.getString("SPU_ID"));
                        bean.setTrademarkId(skuInfo.getString("TM_ID"));
                        bean.setSpuId(skuInfo.getString("SPU_ID"));
                        bean.setCategory3Id(skuInfo.getString("CATEGORY3_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> spuInfo = AsyncDataStream.unorderedWait(

                skuInfo,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    protected String getTable() {
                        return "dim_spu_info";
                    }

                    @Override
                    protected String getTId(TradeSkuOrderBean input) {
                        return input.getSpuId();
                    }

                    @Override
                    protected void addDim(TradeSkuOrderBean bean, JSONObject skuInfo) {

                        bean.setSpuName(skuInfo.getString("SPU_NAME"));

                    }

                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> c3 = AsyncDataStream.unorderedWait(

                spuInfo,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    protected String getTable() {
                        return "dim_base_category3";
                    }

                    @Override
                    protected String getTId(TradeSkuOrderBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    protected void addDim(TradeSkuOrderBean bean, JSONObject skuInfo) {

                        bean.setCategory3Name(skuInfo.getString("NAME"));
                        bean.setCategory2Id(skuInfo.getString("CATEGORY2_ID"));

                    }

                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> c2 = AsyncDataStream.unorderedWait(

                c3,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    protected String getTable() {
                        return "dim_base_category2";
                    }

                    @Override
                    protected String getTId(TradeSkuOrderBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    protected void addDim(TradeSkuOrderBean bean, JSONObject c3) {

                        bean.setCategory2Name(c3.getString("NAME"));
                        bean.setCategory1Id(c3.getString("CATEGORY1_ID"));

                    }

                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> c1 = AsyncDataStream.unorderedWait(

                c2,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    protected String getTable() {
                        return "dim_base_category1";
                    }

                    @Override
                    protected String getTId(TradeSkuOrderBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    protected void addDim(TradeSkuOrderBean bean, JSONObject c2) {

                        bean.setCategory1Name(c2.getString("NAME"));

                    }

                },
                60,
                TimeUnit.SECONDS
        );



       return AsyncDataStream.unorderedWait(

                c1,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    protected String getTable() {
                        return "dim_base_trademark";
                    }

                    @Override
                    protected String getTId(TradeSkuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    protected void addDim(TradeSkuOrderBean bean, JSONObject c1) {

                        bean.setTrademarkName(c1.getString("TM_NAME"));


                    }

                },
                60,
                TimeUnit.SECONDS
        );




    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndJoin(SingleOutputStreamOperator<TradeSkuOrderBean> distinctedStream) {

        return distinctedStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj, ts) -> obj.getTs())
                                .withIdleness(Duration.ofSeconds(20))
                )
                .keyBy(TradeSkuOrderBean::getOrderDetailId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                                value1.setCouponAmount(value1.getCouponAmount().add(value2.getCouponAmount()));
                                value1.setActivityAmount(value1.getActivityAmount().add(value2.getActivityAmount()));

                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context ctx, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {

                                TradeSkuOrderBean bean = elements.iterator().next();
                                bean.setStt(AtguiguiUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(AtguiguiUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setTs(System.currentTimeMillis());

                                out.collect(bean);

                            }
                        });
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> distinctByOrderId(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {

        return stream
                .keyBy(TradeSkuOrderBean::getOrderDetailId)
                .process(new KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean>() {

                    private ValueState<TradeSkuOrderBean> beanState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        beanState = getRuntimeContext().getState(new ValueStateDescriptor<TradeSkuOrderBean>("beanState", TradeSkuOrderBean.class));
                    }

                    @Override
                    public void processElement(TradeSkuOrderBean bean, Context ctx, Collector<TradeSkuOrderBean> out) throws Exception {

                        TradeSkuOrderBean lastBean = beanState.value();

                        if (lastBean != null) {

                            //取出状态中的值，取反
                            lastBean.setActivityAmount(lastBean.getActivityAmount().negate());
                            lastBean.setCouponAmount(lastBean.getCouponAmount().negate());
                            lastBean.setOriginalAmount(lastBean.getOriginalAmount().negate());
                            lastBean.setOrderAmount(lastBean.getOrderAmount().negate());

                            //1.把数据存入到流中
                            out.collect(lastBean);

                            //2.把这条数据放入到状态中

                        }

                        out.collect(bean);
                        beanState.update(bean);


                    }
                });


    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> parseToPojo(DataStreamSource<String> stream) {

        return stream
                .map(new MapFunction<String, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(String json) throws Exception {

                        JSONObject obj = JSON.parseObject(json);

                        return TradeSkuOrderBean.builder()
                                .orderDetailId(obj.getString("id"))
                                .skuId(obj.getString("sku_id"))
                                .originalAmount(obj.getBigDecimal("split_original_amount"))
                                .activityAmount(obj.getBigDecimal("split_activity_amount") == null ? new BigDecimal(0) : obj.getBigDecimal("split_activity_amount"))
                                .couponAmount(obj.getBigDecimal("split_coupon_amount") == null ? new BigDecimal(0) : obj.getBigDecimal("split_coupon_amount"))
                                .ts(obj.getLong("ts") * 1000)
                                .orderAmount(obj.getBigDecimal("split_total_amount"))
                                .build();
                    }
                });

    }
}

/**
 * 1.写异步之前，前面所有代码应该是正常运行
 * 2.异步超时，一般是其他原因导致的超时
 *  1.检测集群是否均正常开启
 *      hadoop redis hbase kafka zk
 *  2.检查 phoenix 中6张维度表是否都在
 *
 *  3.再检测 6 张表是否都有数据
 *
 *  4.去检测
 *
 */

/*
有一些热门商品，会频繁的去维度表中查询数据，效率比较低

缓存

缓存优化：
    先去缓存中查找，缓存中没有，再去数据库中查，把维度保存到缓存中

    1. flink状态（内部缓存）
        好处：速度特别快（本地内存）
        坏处：
            1.占用 flink 的运行内存
            2.当维度发生变化的时候，缓存中的数据不能及时更新

    2. redis，天生用来做缓存
        坏处：外部缓存，每次需要通过网络去读，速度没有内部缓存快
        好处：当维度发生变化的时候，缓存中的数据能够及时更新
            DimAPP 当维度发生变化的时候，可以去 redis 中删除变化的维度信息

----------------------
redis 的数据接口选择：
    1。string
    key(只用id是不行，每张表都肯能会有id)               value
    table+id                                         json格式的字符串
    好处：
        1.读写非常的方便
        2.单独的给每个维度设置 ttl
    坏处：
        1.key 是和 id 数量一致，key 过多，不好管理，有 key 冲突的风险 （解决方案： 可以选择一个单独的数据库）

    2.list
    key                        value
    table名                    [json格式数据，json格式数据]
    好处：key 少，一张表一个 key
    坏处：
        读不方便:需要读取这张表的所有缓存数据，然后在 flink 内部进行遍历，找到需要的数据

      没有办法单独的给每个维度设置 ttl

    3.set
        和 list 相比，多了个去重功能

    4.hash
    key          field      value
    表名          id          json格式字符串
                  id          json格式字符串
    好处：
        key 少 一张表一个 key
        读写方便

     坏处：
        没有办法单独的给每个维度设置ttl


    5.zset
         和list相比，多了个排序功能

 */































































