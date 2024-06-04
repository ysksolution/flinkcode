package com.sjfood.sjfood.gmallrealtime.util;

import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.annotation.NotSink;
import com.sjfood.sjfood.gmallrealtime.bean.TableProcess;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.sink.PhoenixSink;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/7/21:40
 * @Package_name: com.atguigu.gmallrealtime.util
 */
@Slf4j
public class FlinkSinKUtil {

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {


        return new PhoenixSink();
    }

    public static SinkFunction<String> getKafkaSink(String topic) {
        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        //两阶段提交，严格一次的时候
        //服务器要求不超过15分钟，生产者默认一个小时
        //服务器：transaction.max.timeout.ms
        //生产者：transaction.timeout.ms
//        pros.setProperty();
        pros.put("transaction.timeout.ms", 15 * 60 * 1000);

        return new FlinkKafkaProducer<String>(

                "default",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {

                        return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
                    }
                },
                pros,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );


    }


    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getKafkaSink() {


        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        pros.put("transaction.timeout.ms", 15 * 60 * 1000);

        //此时流中需要的数据是Tuple2
        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(

                "default",
                new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> t, @Nullable Long timestamp) {

                        //写入的数据
                        String data = t.f0.toJSONString();
                        //每条数据都带着topic
                        String topic = t.f1.getSinkTable();

                        return new ProducerRecord<>(topic, data.getBytes(StandardCharsets.UTF_8));
                    }
                },
                pros,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );


    }

    public static <T> SinkFunction<T> getClickhouseSink2(String table, Class<T> tClass) {

        // 获取列名
        ArrayList<String> list = new ArrayList<>();
        Field[] fields = tClass.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            name = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE,name);
            list.add(name);
        }
        String columnNames = list.stream().collect(Collectors.joining(","));
        System.out.println("列名："+columnNames);

//        "insert into books(id, title, authors, year) values(?, ?, ?, ?)",
        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("insert into ")
                .append(table)
                .append("(")
                .append(columnNames)
                .append(") values(")
                .append(columnNames.replaceAll("[^,]+","?"))
                .append(")");
        log.warn("clickhouse插入语句："+insertSQL);
//
//
        return JdbcSink.sink(
                insertSQL.toString(),
                //赋值语句
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement ps, T t) throws SQLException {
                        Field[] fields = t.getClass().getDeclaredFields();

                        for (int i = 0,position=1; i < fields.length; i++) {
                            Field field = fields[i];
                            if( field.getAnnotation(NotSink.class) == null) {
                                field.setAccessible(true);
                                Object v = field.get(t);
//                                    System.out.println(field.getName() + "    " + v);
                                ps.setObject(position++,v);
                            }

//
//                            field.setAccessible(true);
//                            String columnName = field.getName();
//                            Object obj = field.get(t);
//                            System.out.println("obj:"+obj);
//                            ps.setObject(position++,obj);
                        }

                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(Constant.CLICKHOUSE_URl)
                        .withDriverName(Constant.CLICKHOUSE_DRIVER)
                        .withUsername("default")
                        .withPassword(null)
                        .build()
        );

    }


    // 用 jdbc 的方式向 clickhouse 写入数据
    public static <T> SinkFunction<T> getClickHouseSink(String table, Class<T> tClass) {

        String driver = Constant.CLICKHOUSE_DRIVER;
        String url = Constant.CLICKHOUSE_URl;

        // 获取列名
        ArrayList<String> list = new ArrayList<>();
        Field[] fields = tClass.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            name = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE,name);
            list.add(name);
        }
        String columns = list.stream().collect(Collectors.joining(","));
        System.out.println("列名："+columns);

// 列名
//        String columns = Arrays
//                .stream(tClass.getDeclaredFields())
//                .filter(f -> {
//                    // 过滤掉在 clickhouse 的表中不需要的属性
//                    // 如果这个属性有 NotSink 这个注解，就不要了
//                    return f.getAnnotation(NotSink.class) == null;
//                })
//                .map(f -> CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, f.getName())
//                )
//                .collect(Collectors.joining(","));
//
//        System.out.println("columns: " + columns);
        //insert into persion(id,name)values(1,'xiaowenqin')


        StringBuffer sql = new StringBuffer();

        sql.append("insert into ")
                .append(table)
                .append("(")
                .append(columns)
                .append(") values(")
                .append(columns.replaceAll("[^,]+","?"))
                .append(")");
        log.warn("clickhouse插入语句："+sql);


//
//        sql
//                .append(" insert into ")
//                .append(table)
//                .append("(")
//                //拼接字段名 pojo的属性名要和表的字段名保持一致
//                //获取一个类：类名.class,Class.forName("全类名) 对象.getClass()  ClassLoader.getSystemClassLoader().
//                .append(columns)
//                .append(")values(")
//                //拼接？
//                .append(columns.replaceAll("[^,]+", "?"))
//                .append(")");
//
//        System.out.println("clickhouse插入语句：" + sql);

        //如果数据库没有设置用户名和密码，传入两个null
         return JdbcSink
                .sink(sql.toString(),
                        new JdbcStatementBuilder<T>() {
                            @Override
                            public void accept(PreparedStatement ps, T t) throws SQLException {

                                //只做一件事：给sql中的占位符赋值
                                //要根据你的 sql 语句来赋值
                                Class<?> tClass = t.getClass();
                                Field[] fields = tClass.getDeclaredFields();
                                try {

                                    for (int i = 0,position=1; i < fields.length; i++) {
                                        Field field = fields[i];
                                        if( field.getAnnotation(NotSink.class) == null) {
                                            field.setAccessible(true);
                                            Object v = field.get(t);
//                                    System.out.println(field.getName() + "    " + v);
                                            ps.setObject(position++,v);
                                        }
                                    }
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                }
                            }
                        },
                        new JdbcExecutionOptions.Builder()
                                .withBatchSize(1024)//批次的大小
                                //每3秒，刷新一次
                                .withBatchIntervalMs(3000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withDriverName(driver)
                                .withUrl(url)
                                .withUsername("default")
                                .withPassword(null)
                                .build()
                );

    }

    private static <T> SinkFunction<T> getJdbcSink(String driver,
                                                   String url,
                                                   String sql,
                                                   String user,
                                                   String password) {
        return JdbcSink
                .sink(sql,
             new JdbcStatementBuilder<T>() {
                    @Override
                public void accept(PreparedStatement ps, T t) throws SQLException {

                        //只做一件事：给sql中的占位符赋值
                        //要根据你的 sql 语句来赋值
                        Class<?> tClass = t.getClass();
                        Field[] fields = tClass.getDeclaredFields();
                        try {

                            for (int i = 0,position=1; i < fields.length; i++) {
                                Field field = fields[i];
                                if( field.getAnnotation(NotSink.class) == null) {
                                    field.setAccessible(true);
                                    Object v = field.get(t);
//                                    System.out.println(field.getName() + "    " + v);
                                    ps.setObject(position++,v);
                                }
                            }
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                 },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1024)//批次的大小
                        //每3秒，刷新一次
                        .withBatchIntervalMs(3000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(driver)
                        .withUrl(url)
                        .withUsername(user)
                        .withPassword(password)
                        .build()
        );


    }
}
