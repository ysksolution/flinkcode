package com.sjfood.sjfood.gmallrealtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.TradeSkuOrderBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
@Slf4j
public class copy_Dws_09_DwsTradeSkuOrderWindow extends BaseAPPV1 {

    public static void main(String[] args) {
        new copy_Dws_09_DwsTradeSkuOrderWindow().init(
                5009, 2, "Dws_09_DwsTradeSkuOrderWindow", Constant.DWD_TRADE_ORDER_DETAIL
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1.封装数据到pojo中
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(stream);
//        beanStream.print();

//        //2.按照 order_detail_id 去重
        SingleOutputStreamOperator<TradeSkuOrderBean> distinctedStream = distinctByOrderId(beanStream);
//        distinctedStream.print();
//
//        //3.开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> join = windowAndJoin(distinctedStream);
//        join.print();
//
//        //4.补充维度信息
        joinDim(join);

    }




    /*
        补充维度：事实表和维度比表关联，lookup join
        1.读取数据流数据，获取sku_id
     */
    private void joinDim(SingleOutputStreamOperator<TradeSkuOrderBean> bean) {

         bean.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

            private Connection conn;

            @Override
            public void open(Configuration parameters) throws Exception {
                conn = DruidDSUtil.getPhoenixConn();
            }

            @Override
            public void close() throws Exception {

                if(conn != null){
                    conn.close();
                }
            }

            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean value) throws Exception {

//                System.out.println("value:"+value);
                String sku_id = value.getSkuId();

                System.out.println("sku_id="+sku_id);

                String dimTable = "dim_sku_info";
                // 读取Phoenix中维表数据
                String sql = "select * from " + dimTable + "  where id = ? " ;

                PreparedStatement ps = conn.prepareStatement(sql);
                // 给占位符赋值
                ps.setObject(1,sku_id);

                ResultSet resultSet = ps.executeQuery();

                // 处理结果集
                ResultSetMetaData metaData = resultSet.getMetaData();
                while (resultSet.next()){
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i);
                        Object obj = resultSet.getObject(i);
                        System.out.println(columnName +":"+obj);
                    }
                }

                // 取出dim需要的数据：dim_sku_info，需要 SPU_ID
                ps.close();
                return value;

            }
        });


    }



    /*
        开窗聚合:
            1.加水印
            2.keyby
            3.开窗
            4.reduce（增量）/process（全量）
                   1. ReduceFunction（计算过程）
                   2. ProcessWindowFunction（获取窗口信息）

     */
    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndJoin(SingleOutputStreamOperator<TradeSkuOrderBean> distinctedStream) {

        // 水印
        WatermarkStrategy<TradeSkuOrderBean> watermarkStrategy = WatermarkStrategy
                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, ts) -> event.getTs())
                .withIdleness(Duration.ofSeconds(10));

        return distinctedStream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(TradeSkuOrderBean::getOrderDetailId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                                value1.setActivityAmount(value1.getActivityAmount().add(value2.getActivityAmount()));
                                value1.setCouponAmount(value1.getCouponAmount().add(value2.getCouponAmount()));
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                return value1;
                            }
                        }
                        , new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context ctx, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                                TradeSkuOrderBean bean = elements.iterator().next();
                                bean.setStt(AtguiguiUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(AtguiguiUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setTs(System.currentTimeMillis());
                                out.collect(bean);
                            }
                        }
                );
    }


    /*
    去重orderDetailId
    左连接会产生回撤流，所以需要抵消数据
    id  split_original_Amount  split_coupon_Amount split_activity_Amount split_orderAmount coupon_id  activity_id
1->   1     100                      10                    10                 80               null         null
2->   null
3->   1     100                      10                    10                 80               10001        null
4->   null
5->   1     100                      10                    10                 80               10001        0011


1->   1     100                      10                    10                 80               null         null   state.value = '2024-04-12'
1->   1    -100                     -10                   -10                -80               null         null   state.value = '2024-04-12'
3->   1     100                      10                    10                 80               10001        null
5->   1     100                      10                    10                 80               10001        0011

    （1）过滤掉null
    （2）比如上面，有三条数据，需要去重两条数据
    （3）
      1->   1     100                      10                    10                 80               null         null   state.value = '2024-04-12'
      2->   1    -100                     -10                   -10                -80               null         null   state.value = '2024-04-12'
      3->   1     100                      10                    10                 80               10001        null

      写入第三条数据时，需要写将上一条数据取反写入（对应null值），再写入第三条数据

 */
    private SingleOutputStreamOperator<TradeSkuOrderBean> distinctByOrderId(SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {

        return beanStream.keyBy(TradeSkuOrderBean::getOrderDetailId)
                .process(new KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private ValueState<TradeSkuOrderBean> beanState;
                    @Override
                    public void open(Configuration parameters) throws Exception {

                        ValueStateDescriptor<TradeSkuOrderBean> valueStateDescriptor = new ValueStateDescriptor<>("ValueStateDescriptor",TradeSkuOrderBean.class);
                        beanState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(TradeSkuOrderBean bean, Context ctx, Collector<TradeSkuOrderBean> out) throws Exception {

                        TradeSkuOrderBean lastBean = beanState.value();

                        // 第一条数据
                        if(lastBean != null){
                            lastBean.setOriginalAmount(bean.getOriginalAmount().negate());
                            lastBean.setActivityAmount(bean.getActivityAmount().negate());
                            lastBean.setCouponAmount(bean.getCouponAmount().negate());
                            lastBean.setOrderAmount(bean.getOrderAmount().negate());

                            out.collect(lastBean);
                        }

                        out.collect(bean);
                        beanState.update(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> parseToPojo(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, TradeSkuOrderBean>() {
            @Override
            public void flatMap(String value, Collector<TradeSkuOrderBean> out) throws Exception {
                if(value != null){
                    JSONObject obj = JSONObject.parseObject(value);
                    TradeSkuOrderBean data = TradeSkuOrderBean.builder()
                            .orderDetailId(obj.getString("id"))
                            .originalAmount(obj.getBigDecimal("split_original_amount")==null ? new BigDecimal(0):obj.getBigDecimal("split_original_amount"))
                            .orderAmount(obj.getBigDecimal("split_total_amount")==null?new BigDecimal(0):obj.getBigDecimal("split_total_amount"))
                            .couponAmount(obj.getBigDecimal("split_coupon_amount")==null?new BigDecimal(0):obj.getBigDecimal("split_coupon_amount"))
                            .activityAmount(obj.getBigDecimal("split_activity_amount")==null?new BigDecimal(0):obj.getBigDecimal("split_activity_amount"))
                            .skuId(obj.getString("sku_id"))
                            .skuName(obj.getString("sku_name"))
                            .ts(obj.getLong("ts")*1000)
                            .build();
                    out.collect(data);
                }
            }
        });

    }
}
