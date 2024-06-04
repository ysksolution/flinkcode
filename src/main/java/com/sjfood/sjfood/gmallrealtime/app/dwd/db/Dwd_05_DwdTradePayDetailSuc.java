package com.sjfood.sjfood.gmallrealtime.app.dwd.db;


import com.sjfood.sjfood.gmallrealtime.app.BaseSQLAPP;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/9/11:02
 * @Package_name: com.atguigu.gmallrealtime.app.dwd.db
 */
public class Dwd_05_DwdTradePayDetailSuc extends BaseSQLAPP {

    public static void main(String[] args) {
        new Dwd_05_DwdTradePayDetailSuc().init(
                3005,2,"Dwd_05_DwdTradePayDetailSuc"
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(1820));
        //1.读取ods_db
        readOdsDb(tEnv,"Dwd_05_DwdTradePayDetailSuc");
        //2.读取字典表
        readBaseDic(tEnv);
        //3.读取dwd_trade_order_detail
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

        //4.从ods_db过滤payment_info
        Table payment_info = tEnv.sqlQuery("select " +
                " data['user_id'] user_id ," +
                " data['order_id'] order_id, " +
                " data['payment_type']  payment_type," +
                " data['callback_time']  callback_time," +
                "ts," +
                "pt" +
                " from ods_db " +
                " where `database` = 'gmall2022' " +
                " and `table` = 'payment_info' " +
                " and `type` = 'insert' " +
                "");
        tEnv.createTemporaryView("payment_info",payment_info);
//        payment_info.execute().print();

        //5.3张表join
        Table result = tEnv.sqlQuery("select " +
                "od.id order_detail_id," +
                "od.order_id," +
                "od.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "od.province_id," +
                "od.activity_id," +
                "od.activity_rule_id," +
                "od.coupon_id," +
                "pi.payment_type," +
                "dic.dic_name payment_type_name," +
                "pi.callback_time," +
                "od.source_id," +
                "od.source_type source_type_code," +
                "od.source_type_name," +
                "od.sku_num," +
                "od.split_original_amount," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount split_payment_amount," +
                "pi.ts" +
                " from payment_info pi " +
                " join  dwd_trade_order_detail od " +
                " on pi.order_id = od.order_id " +
                " join base_dic for system_time as of pi.pt as dic" +
                " on dic.dic_code = pi.payment_type ");
        tEnv.createTemporaryView("result",result);

        //6.写出到kafka
        tEnv.executeSql("create table if not exists dwd_trade_pay_detail_suc(" +
                        "order_detail_id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "payment_type_code string," +
                        "payment_type_name string," +
                        "callback_time string," +
                        "source_id string," +
                        "source_type_code string," +
                        "source_type_name string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_payment_amount string," +
                        "ts string," +
                        "primary key(order_detail_id) not enforced" +
                ")"+SQLUtil.getUpsertKafkaDDL(Constant.DWD_TRADE_PAY_DETAIL_SUC,"json"));

        result.executeInsert("dwd_trade_pay_detail_suc");


    }
}

































