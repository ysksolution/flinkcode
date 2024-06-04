package com.sjfood.sjfood.gmallrealtime.util;

import com.sjfood.sjfood.gmallrealtime.common.Constant;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/8/9:18
 * @Package_name: com.atguigu.gmallrealtime.util
 */
public class SQLUtil {

    public static String getKafkaSinkDDL(String topic, String... type) {

        String f = "json";
        if (type.length > 0) {
            f = type[0];
        }
        return "with(" +
                "'connector' = 'kafka'," +
                "'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "'topic' = '" + topic + "'," +
                "'format' = '" + f + "'" +
                ")";

    }

    public static String getKafkaSourceDDL(String topic, String groupId, String... type) {

        String typeFormat = "csv";

        if(type.length>0){
            typeFormat = type[0];
        }

        return " WITH (" +
                "'connector' = 'kafka', " +
                "'topic' = '" + topic + "', " +
                "'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
                "'properties.group.id' = '" + groupId + "'," +
                "'scan.startup.mode' = 'earliest-offset'," +
                "'format' = '"+typeFormat+"'"+
                ")";

    }


    public static String getUpsertKafkaDDL(String topic, String... type) {

        String f = "json";
        if (type.length > 0) {
            f = type[0];
        }

        return "with(" +
                "'connector' = 'upsert-kafka'," +
                "'topic' = '" + topic + "'," +
                "'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "'key.format' = '" + f + "'," +
                "'value.format' = '" + f + "', " +
                "'sink.parallelism' = '2'" +
                ")";
    }

}
