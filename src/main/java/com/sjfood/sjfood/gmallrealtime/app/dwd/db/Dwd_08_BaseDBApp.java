package com.sjfood.sjfood.gmallrealtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.TableProcess;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import com.sjfood.sjfood.gmallrealtime.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;


/**
 * @Author: YSKSolution
 * @Date: 2022/11/11/9:17
 * @Package_name: com.atguigu.gmallrealtime.app.dwd.db
 */
public class Dwd_08_BaseDBApp extends BaseAPPV1 {

    public static void main(String[] args) {

        new Dwd_08_BaseDBApp().init(
                3008,
                2,
                "Dwd_08_BaseDBApp",
                Constant.TOPIC_ODS_DB
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {


        //1.etl
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);

        //2.通过flink cdc 读配置数据
        SingleOutputStreamOperator<TableProcess> tpStream = readTableSource(env);
//        tpStream.print();

        //3.数据流和配置流做connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> streams = connectStream(etledStream, tpStream);
        streams.print();

        //4.过滤掉不需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream = delNoNeedColumns(streams);
//        resultStream.print();

        //5.不同的表写出到不同的topic中
        writeToKafka(resultStream);

    }

    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream) {

        resultStream.addSink(FlinkSinKUtil.getKafkaSink());


    }


    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> delNoNeedColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {

        return stream.map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> tp) throws Exception {

                JSONObject data = tp.f0;

                //data中的 key 在 columns 中 存在保留，不存在就删除
                List<String> columns = Arrays.asList(tp.f1.getSinkColumns().split(","));
//                System.out.println("配置表的key："+columns.toString());

                Set<String> keySet = data.keySet();
//                System.out.println("数据流的key："+keySet);

                data.keySet().removeIf(key -> !columns.contains(key) && "op_type" != key);
                return tp;
            }
        });
    }


    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStream(SingleOutputStreamOperator <JSONObject> dataStream, SingleOutputStreamOperator <TableProcess> tpStream){

        //1.配置流做成广播流
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);

        BroadcastStream<TableProcess> bcStream = tpStream.broadcast(tpStateDesc);

        return dataStream

                .connect(bcStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {

                    //进行预加载
                    private HashMap<String, TableProcess> tpMap;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        preLoadTableProcess();

                    }

                    //对配置信息进行预加载
                    private void preLoadTableProcess() {

                        tpMap = new HashMap<>();
                        //获取
                        Connection conn = JdbcUtil.getMySQLConnection();
                        //查询一个表中所有行的数据
                        //写一个通用的sql，可以会存在占位符
                        List<TableProcess> tpList = JdbcUtil.queryList(conn,
                                "select * from gmall_config.table_process",
                                TableProcess.class,
                                true);

                            //把每行数据放入到map中
                            for (TableProcess tp : tpList) {

                            String key = tp.getSourceTable() + ":" + tp.getSourceType();
                            tpMap.put(key,tp);
                        }

                        //日志中打印预加载的信息
                        String msg = "配置信息预加载：\n\t\t";
                        for (Map.Entry<String, TableProcess> kv : tpMap.entrySet()) {
                            String key = kv.getKey();
                            TableProcess value = kv.getValue();
                            msg += key + "=>" + value + "\n\t\t";

                        }
//                        log.warn(msg);

                    }


                    @Override
                    public void processElement(JSONObject obj, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {

                        //根据key来获取对应的配置信息
                        String table = obj.getString("table");
                        String type = obj.getString("type");

                        JSONObject data = obj.getJSONObject("data");
                        JSONObject old = obj.getJSONObject("old");

                        String key = table + ":" + type;

                        if ("coupon_use".equals(table) && "update".equals(type)) {

                                String newStatus = data.getString("coupon_status");
                                String oldStatus = old.getString("coupon_status");

                                if ("1401".equals(oldStatus) && "1402".equals(newStatus)) {
                                    key += "{\"data\": {\"coupon_status\": \"1402\"}, \"old\": {\"coupon_status\": \"1401\"}}";

                                } else if ("1402".equals(oldStatus) && "1403".equals(newStatus)) {
                                    key += "{\"data\": {\"used_time\": \"not null\"}}";
                                    System.out.println(key);

                                }

                            }
                            ReadOnlyBroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);

                                TableProcess tp = tpState.get(key);

                            //广播状态中没有获取对应的配置信息，去HashMap中获取
                            if (tp == null) {

                                tp = tpMap.get(key);
                              /*  if (tpMap != null) {
                                    tp = tpMap.get(key);
                                }*/

                            }

                            if (tp != null) {
                                //把 type 的值放入到 data 中，到后期要使用
                                data.put("op_type",obj.getString("type"));
                                out.collect(Tuple2.of(data,tp));

                            }
                    }

                    @Override
                    public void processBroadcastElement(TableProcess tp, Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {

                        String key = tp.getSourceTable() + ":" + tp.getSourceType()
                                + (tp.getSinkExtend() == null ? "" : tp.getSinkExtend());

                        BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                        state.put(key, tp);
                    }
                });

    }



    private SingleOutputStreamOperator<TableProcess> readTableSource (StreamExecutionEnvironment env){
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop162")//mysql地址
                .port(3306)//myql端口
                .databaseList("gmall_config") // set captured database
                .tableList("gmall_config.table_process") // set captured table
                .username("root")
                .password("aaaaaa")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-cdc")
                .map(new MapFunction<String, TableProcess>() {
                    @Override
                    public TableProcess map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);
                        String op = obj.getString("op");
                        String beforeOrAfter = "after";

                        if ("d".equals(op)) {
                            beforeOrAfter = "before";
                        }

                        TableProcess tp = obj.getObject(beforeOrAfter, TableProcess.class);
                        tp.setOp(op);
                        return tp;
                    }
                });

    }


    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {

        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                //1.对流中的数据清洗：etl
                /*
                    1.都应该是json格式，格式不对是脏数据
                    2.库是gmall2022
                 */

                try {
                    JSONObject obj = JSON.parseObject(value.replaceAll("bootstrap-",""));

                    String type = obj.getString("type");
                    String data = obj.getString("data");

                    return "gmall2022".equals(obj.getString("database"))
                            && obj.getString("table")  != null
                            && ("insert".equals(type) || "update".equals(type))
                            && data != null
                            && data.length() > 2;
                } catch (Exception e) {

                    System.out.println("数据格式有误，不是json格式数据:" + value);
                    e.printStackTrace();
                    return false;
                }
            }
        })
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value.replaceAll("bootstrap-",""));
                    }
                });


}

}