package com.sjfood.sjfood.gmallrealtime.app.dws;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.TradeSkuOrderBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.AtguiguiUtil;
import com.sjfood.sjfood.gmallrealtime.util.DimUtil;
import com.sjfood.sjfood.gmallrealtime.util.DruidDSUtil;
import com.sjfood.sjfood.gmallrealtime.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.Duration;

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
public class Dws_09_DwsTradeSkuOrderWindow extends BaseAPPV1 {

    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow().init(
                4009, 2, "Dws_09_DwsTradeSkuOrderWindow", Constant.DWD_TRADE_ORDER_DETAIL
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1.封装数据到pojo中
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(stream);

        //2.按照 order_detail_id 去重
        SingleOutputStreamOperator<TradeSkuOrderBean> distinctedStream = distinctByOrderId(beanStream);

        //3.开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> join = windowAndJoin(distinctedStream);
        join.print();

        //4.补充维度信息
//        joinDim(join);

    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> joinDim(SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDim) {



      /*
        流与维度表的 join: 需要自己实现
        每来一条数据, 根据需要的条件去查询对应的维度信息: 本质执行一个 sql 语句
       */
        return beanStreamWithoutDim
             .map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

//                 private Connection conn1;
                 private Jedis redisClient;
                 private Connection conn;

                 @Override
                 public void open(Configuration parameters) throws Exception {

                     //1.创建到 phoenix 的连接
                     conn = DruidDSUtil.getPhoenixConn();
                     //2.创建到 redis 的连接
                     redisClient = RedisUtil.getRedisClient();

                 }

                 @Override
                 public TradeSkuOrderBean map(TradeSkuOrderBean bean) throws Exception {

                     //1.sku_info
                     JSONObject skuInfo = DimUtil.readDim(redisClient,conn,"dim_sku_info",bean.getSkuId());

                     bean.setSkuId(skuInfo.getString("SKU_ID"));
                     bean.setSkuName(skuInfo.getString("SKU_NAME"));
                     bean.setTrademarkId(skuInfo.getString("TM_ID"));
                     bean.setSpuId(skuInfo.getString("SPU_ID"));
                     bean.setCategory3Id(skuInfo.getString("CATEGORY3_ID"));

                    //2.spu_info
                     JSONObject spuInfo = DimUtil.readDim(redisClient,conn, "dim_spu_info", bean.getSpuId());
                     bean.setSpuName(spuInfo.getString("SPU_NAME"));

                     //3.c3
                     JSONObject c3 = DimUtil.readDim(redisClient, conn,"dim_base_category3", bean.getCategory3Id());
                     bean.setCategory3Name(c3.getString("NAME"));
                     bean.setCategory2Id(c3.getString("CATEGORY2_ID"));

                     //4.c2
                     JSONObject c2 = DimUtil.readDim(redisClient,conn, "dim_base_category2", bean.getCategory2Id());
                     bean.setCategory2Name(c2.getString("NAME"));
                     bean.setCategory1Id(c2.getString("CATEGORY1_ID"));

                     //5.c1
                     JSONObject c1 = DimUtil.readDim(redisClient,conn, "dim_base_category1", bean.getCategory1Id());
                     bean.setCategory1Name(c1.getString("NAME"));

                     //6.dim_base_trademark
                     JSONObject baseTrademark = DimUtil.readDim(redisClient,conn, "dim_base_trademark", bean.getTrademarkId());
                     bean.setTrademarkName(baseTrademark.getString("TM_NAME"));

                     return bean;
                 }
             });

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
