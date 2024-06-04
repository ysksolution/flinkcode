package com.sjfood.sjfood.gmallrealtime.common;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/4/16:00
 * @Package_name: com.atguigu.gmallrealtime.common
 */
public class Constant {

    public static final String KAFKA_BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306?useSSL=false";
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASSWD = "mivbAs7Awc";


    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_URl = "jdbc:clickhouse://hadoop102:8123/gmall";



    public static final String TOPIC_ODS_DB = "ods_db";
    public static final String TOPIC_ODS_LOG = "ods_log";

    public static final String DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String DWD_TRAFFIC_ACTION = "dwd_traffic_action";

    public static final String DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String DWD_TRADE_CANCEL_DETAIL = "dwd_trade_cancel_detail";
    public static final String DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund" ;
    public static final String DWD_TRADE_PAY_DETAIL_SUC = "dwd_trade_pay_detail_suc";

    public static final String DWD_USER_REGISTER = "dwd_user_register";

    public static final int TTL_TWO_days = 2 * 24 * 60 * 60;
}
