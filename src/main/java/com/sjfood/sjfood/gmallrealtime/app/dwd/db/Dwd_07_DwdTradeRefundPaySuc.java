package com.sjfood.sjfood.gmallrealtime.app.dwd.db;


import com.sjfood.sjfood.gmallrealtime.app.BaseSQLAPP;
import com.sjfood.sjfood.gmallrealtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/9/14:23
 * @Package_name: com.atguigu.gmallrealtime.app.dwd.db
 */


/**
 * 退款成功事务事实表
 * 退款表：refund_payment，update && refund_status && old.refund_status is not null
 *
 * 想查看是谁退单：order_refund_info update && refund.status = 0705 && old.refund_status is not null
 *
 * 想看省份id ：order_info ：update && data.order_status=1006 && old.refund_status is not null
 *
 * 维度退化：payment_type
 */

public class Dwd_07_DwdTradeRefundPaySuc extends BaseSQLAPP {

    public static void main(String[] args) {
        new Dwd_07_DwdTradeRefundPaySuc().init(
                3007,2,"Dwd_07_DwdTradeRefundPaySuc"
        );
    }


    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(1820));

        //1.读取ods_db
        readOdsDb(tEnv,"Dwd_07_DwdTradeRefundPaySuc");
        //2.读取字典表
        readBaseDic(tEnv);
        //3.从ods_db中过滤refund_payment退款表
        Table refundPaymet = tEnv.sqlQuery(" select " +
                "data['id'] id," +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['payment_type'] payment_type," +
                "data['callback_time'] callback_time," +
                "data['total_amount'] total_amount," +
                "data['refund_status'] refund_status," +
                "pt," +
                "ts" +
                " from ods_db " +
                " where `database` = 'gmall2022' " +
                " and `table` = 'refund_payment' " +
//                " and `type` = 'update' " +
//                " and `old`['refund_status']  is not null " +
//                " and `data`['refund_status'] = '0705' " +
                "");
        tEnv.createTemporaryView("refund_payment",refundPaymet);
//        refundPaymet.execute().print();
        //4.从ods_db中过滤order_refund_info
        Table orderRefundInfo = tEnv.sqlQuery(" select " +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['refund_num'] refund_num," +
                "`old`" +
                " from ods_db " +
                " where `database` = 'gmall2022' " +
                " and `table` = 'order_refund_info' " +
             /*   " and `type` = 'update' " +
                " and `old`['refund_status']  is not null " +
                " and `data`['refund_status'] = '0705' " +*/
                "");
        tEnv.createTemporaryView("order_refund_info",orderRefundInfo);
//        orderRefundInfo.execute().print();
        //5.从ods_db中过滤order_info
        Table orderInfo = tEnv.sqlQuery(" select " +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['province_id'] province_id \n" +
                " from ods_db " +
                " where `database` = 'gmall2022' " +
                " and `table` = 'order_info' " +
                " and `type` = 'update' " +
                " and `old`['order_status']  is not null " +
                " and `data`['order_status'] = '1006' " +
                "");
        tEnv.createTemporaryView("order_info",orderInfo);
//        orderInfo.execute().print();
        //6.Join 4 张表
        Table result = tEnv.sqlQuery("select " +
                "rp.id,\n" +
                "oi.user_id,\n" +
                "rp.order_id,\n" +
                "rp.sku_id,\n" +
                "oi.province_id,\n" +
                "rp.payment_type,\n" +
                "dic.dic_name payment_type_name,\n" +
                "date_format(rp.callback_time,'yyyy-MM-dd') date_id,\n" +
                "rp.callback_time,\n" +
                "ri.refund_num,\n" +
                "rp.total_amount,\n" +
                "rp.ts \n" +
                " from refund_payment rp " +
                " join order_refund_info ri " +
                " on rp.order_id=ri.order_id and rp.sku_id = ri.sku_id " +
                " join order_info oi on oi.id = rp.order_id " +
                " join base_dic for system_time as of rp.pt as dic on rp.payment_type = dic.dic_code " +
                "");
        tEnv.createTemporaryView("result",result);
//        result.execute().print();
        //7.写出到kafka中
        tEnv.executeSql("create table dwd_trade_refund_pay_suc(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts string) " + SQLUtil.getKafkaSinkDDL("dwd_trade_refund_pay_suc","json"));
        result.executeInsert("dwd_trade_refund_pay_suc");
        



    }


}
