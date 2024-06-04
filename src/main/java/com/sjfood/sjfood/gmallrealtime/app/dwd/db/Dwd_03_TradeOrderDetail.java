package com.sjfood.sjfood.gmallrealtime.app.dwd.db;

import com.sjfood.sjfood.gmallrealtime.app.BaseSQLAPP;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/9/8:47
 * @Package_name: com.atguigu.gmallrealtime.app.dwd.db
 */
public class Dwd_03_TradeOrderDetail extends BaseSQLAPP {

    public static void main(String[] args) {

        new Dwd_03_TradeOrderDetail().init(
                3003,2,"Dwd_03_TradeOrderDetail"
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        //1.读取ods_db数据
        readOdsDb(tEnv,"Dwd_03_TradeOrderDetail");
        //2.读取字典表
        readBaseDic(tEnv);
        //3.过滤order_detail数据（insert新增数据）
        Table order_detail = tEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['sku_name'] sku_name," +
                "data['create_time'] create_time," +
                "data['source_id'] source_id," +
                "data['source_type'] source_type," +
                "data['sku_num'] sku_num," +
                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount," +
                "data['split_total_amount'] split_total_amount," +
                "data['split_activity_amount'] split_activity_amount," +
                "data['split_coupon_amount'] split_coupon_amount," +
                "ts," +
                " pt " +
                "from `ods_db` "+
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail' " +
                "and `type` = 'insert'");
        tEnv.createTemporaryView("order_detail",order_detail);
//        order_detail.execute().print();

        //4.过滤order_info,(insert)
        Table order_info = tEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['province_id'] province_id " +
                " from `ods_db` " +
                " where `database` = 'gmall' " +
                " and `table` = 'order_info' " +
                " and `type` = 'insert'");
        tEnv.createTemporaryView("order_info", order_info);
//        order_info.execute().print();

        //5.过滤order_detail_activity（insert）
        Table orderDetailActivity = tEnv.sqlQuery("select " +
                "data['order_detail_id'] order_detail_id," +
                "data['activity_id'] activity_id," +
                "data['activity_rule_id'] activity_rule_id" +
                " from `ods_db` " +
                " where `database` = 'gmall'  " +
                " and `table` = 'order_detail_activity'" +
                " and `type` = 'insert'");
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
//        orderDetailActivity.execute().print();
        //6.过滤order_detail_cupon（insert）
        Table orderDetailCoupon = tEnv.sqlQuery(" select " +
                " data['order_detail_id'] order_detail_id," +
                "data['coupon_id'] coupon_id" +
                " from `ods_db` "+
                " where `database` = 'gmall' " +
                " and `table` = 'order_detail_coupon' " +
                " and `type` = 'insert'");
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
//        orderDetailCoupon.execute().print();
        //7.把五张表join到一起
        Table resultTable = tEnv.sqlQuery(" select " +
                "od.id," +
                "od.order_id," +
                "oi.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "oi.province_id," +
                "act.activity_id," +
                "act.activity_rule_id," +
                "cou.coupon_id," +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id," +
                "od.create_time," +
                "od.source_id," +
                "od.source_type," +
                "dic.dic_name source_type_name," +
                "od.sku_num," +
                "od.split_original_amount," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount," +
                "od.ts " +
                " from order_detail od " +
                " join order_info oi " +
                " on od.order_id = oi.id " +
                " left join order_detail_activity act " +
                " on od.id = act.order_detail_id " +
                " left join order_detail_coupon cou " +
                " on od.id = cou.order_detail_id " +
                " join `base_dic` for system_time as of od.pt as dic" +
                " on od.source_type = dic.dic_code" );

        tEnv.createTemporaryView("result_table", resultTable);

//        resultTable.execute().print();

        //8.创建一个动态表与 Kafka 的topic关联
        tEnv.executeSql( "create table dwd_trade_order_detail(" +
                "id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "date_id string," +
                "create_time string," +
                "source_id string," +
                "source_type string," +
                "source_type_name string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_total_amount string," +
                "ts string," +
                "primary key(id) not enforced)" + SQLUtil.getUpsertKafkaDDL(Constant.DWD_TRADE_ORDER_DETAIL,"json"));

        // 在代码上，输出一张表最直接的方法，就是调用 Table 的方法 executeInsert()方法将一个 Table 写入到注册过的表中，方法传入的参数就是注册的表名

        //9.join的结果写入到kafka中
        resultTable.executeInsert("dwd_trade_order_detail");

    }
}



/*
 *会有哪些对应的事实表：
 * order_detail:新增若干数据
 * 内连接
 * order_info:下单动作，会新增一条数据
 * 左连接，订单详情有的会参加活动，有的不会，但是下单的数据我们都要
 * order_detail_activity:新增若干数据
 * 左连接
 * order_detail_cupon:新增若干数据
 * order_detail中有source_type,所以需要使用字典表,使用lookup join
 */
