package com.sjfood.sjfood.gmallrealtime.app.dwd.db;


import com.sjfood.sjfood.gmallrealtime.app.BaseSQLAPP;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/9/10:13
 * @Package_name: com.atguigu.gmallrealtime.app.dwd.db
 */
public class Dwd_04_TradeCancelDetail extends BaseSQLAPP {

    public static void main(String[] args) {
     new Dwd_04_TradeCancelDetail().init(
             3004,2,"Dwd_04_TradeCancelOrder"
     );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        //1.读取ods_db数据
        readOdsDb(tEnv,"dwd_trade_order_detail ");

        //2.过滤order_info 数据
        Table order_info = tEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['province_id'] province_id, " +
                "data['operate_time'] operate_time," +
                "ts " +
                " from `ods_db` " +
                " where `database` = 'gmall2022' " +
                " and `table` = 'order_info' " +
                " and `type` = 'update'"+
                " and `old`['order_status'] is not null "+
                " and `data`['order_status'] = '1003' "+
                "");
        tEnv.createTemporaryView("order_info", order_info);
//        order_info.execute().print();

        //3.读取下单事务事实表
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
                "ts string,"+
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_total_amount string)" + SQLUtil.getKafkaSourceDDL(Constant.DWD_TRADE_ORDER_DETAIL,"Dwd_04_TradeCancelDetail","json"));
//        tEnv.sqlQuery("select * from dwd_trade_order_detail").execute().print();
//        //4.order_info 和下单事实事实表进行join
        Table result = tEnv.sqlQuery(" select " +
                "od.id," +
                "od.order_id," +
                "oi.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "od.province_id," +
                "od.activity_id," +
                "od.activity_rule_id," +
                "od.coupon_id," +
                "od.date_id," +
                "od.create_time," +
                "od.source_id," +
                "od.source_type," +
                "od.source_type_name," +
                "od.sku_num," +
                "od.ts,"+
                "od.split_original_amount," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount" +
                " from order_info oi " +
                " join `dwd_trade_order_detail` od" +
                " on oi.id = od.order_id"
        );
//        result.execute().print();

        //5.写出到kafka中
        tEnv.executeSql("create table dwd_trade_cancel_detail(" +
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
                "cancel_time string," +
                "source_id string," +
                "source_type_code string," +
                "source_type_name string," +
                "sku_num string," +
                "ts string,"+
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_total_amount string," +
                "primary key(id) not enforced" +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.DWD_TRADE_CANCEL_DETAIL,"json"));

        result.executeInsert("dwd_trade_cancel_detail");
    }
}
