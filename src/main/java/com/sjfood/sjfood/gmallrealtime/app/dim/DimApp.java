package com.sjfood.sjfood.gmallrealtime.app.dim;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.TableProcess;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import com.sjfood.sjfood.gmallrealtime.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/4/11:34
 * @Package_name: com.atguigu.gmall.realtime.app.dim
 */
@Slf4j
public class DimApp extends BaseAPPV1 {

    public static void main(String[] args) {

        new DimApp().init(
                2000,
                2,
                "DimApp",
                Constant.TOPIC_ODS_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        /**
         * stream是真正的数据流
         * env只是一个执行环境
         */
        //这里完成你业务逻辑
//        stream.print();
        //1.对流中的数据进行清洗，过滤出json格式的数据
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
//        etledStream.print();

        //2.读取配置表的数据
        //为什么前面的业务数据不使用flink cdc？
        //配置数据直接使用了flink cdc，日志数据使用了maxwell+kafka，
        //历史原因，数据采集工作已经有一些年头了，从离线数仓就开始使用了，那我们现在要用的数据已经被采集到kafka中了，
        // 数据采集已经是一个比较成熟的系统了，那我们就采用了原先的方案，新的方案现在是全部采用flink cdc

        SingleOutputStreamOperator<TableProcess> tpStream = readTableSource(env);
//        tpStream.print();

        //3.因为dim层的数据是存储在phoenix,所以需要根据配置信息在phoenix中建表
        tpStream= createTableOnPhoenix(tpStream);
//        tpStream.print();

        //4.数据流和配置做connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStreams = connectStreams(etledStream, tpStream);
//        connectStreams.print();

        //5.删除掉不需要的列
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream = delNoNeedColumns(connectStreams);
//        resultStream.print();
//
//        //6.把不同的数据写出到phoenix中的不同的表中
        writeToPhoenix(resultStream);

    }

    private void writeToPhoenix(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream) {

        /**
         *  没有专门的 phoenix sink ，所以需要自定义
         *  ①能不能使用jdbcSink？
         *  不能，如果使用jdbc sink ，流中所有数据只能写入到一个表中，因为我们流中有多张表，所以不能使用jdbc sink
         *  ②自定义 sink
         *
         */
        resultStream.addSink(FlinkSinKUtil.getPhoenixSink());
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
                data.keySet().removeIf(key -> !columns.contains(key) && !"op_type".equals(key));
                return tp;
            }
        });

    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStreams(SingleOutputStreamOperator<JSONObject> dataStream, SingleOutputStreamOperator<TableProcess> tpStream) {

        //广播：key：user_info:ALL
        //广播：value：TableProcess
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);

        //1.把配置流做成广播流
        BroadcastStream<TableProcess> bcStream = tpStream.broadcast(tpStateDesc);

        //2.数据流去connect广播流
        return dataStream
                .connect(bcStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject,TableProcess>>() {

                    private HashMap<String, TableProcess> tpMap;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        /**
                         * 先把所有的配置信息全部加载起来
                         * 预加载配置信息
                         * 放入什么状态？不能放入状态：因为再open中不能使用状态
                         * 放入 HashMap 中
                         * 获取配置信息的时候，先从状态中获取，状态中没有再从 HashMap 中获取
                         */
//                        System.out.println("open方法");
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

                    //4.当数据信息来的时候，从广播状态读取配置信息
                    @Override
                    public void processElement(JSONObject obj, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {

                        ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);

                        //广播状态是map，根据key获取值
                        String key = obj.getString("table") + ":ALL";
                       // System.out.println("数据流的key:"+key);

                        TableProcess tp = state.get(key);

                        //System.out.println("匹配到的数据是："+tp);

                        //广播状态中没有获取对应的配置信息，去HashMap中获取
                        if (tp == null) {

                            tp = tpMap.get(key);

                        }



                        if (tp != null) {
                            //把 type 的值放入到 data 中，到后期要使用
                            JSONObject data = obj.getJSONObject("data");
                            data.put("op_type",obj.getString("type"));
                            out.collect(Tuple2.of(data,tp));

                        }

                    }

                    //3.把配置信息放入到广播状态
                    @Override
                    public void processBroadcastElement(TableProcess tp, Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        String key = tp.getSourceTable() + ":" + tp.getSourceType();
//                        System.out.println("广播流的key:"+key);
                        //获取状态
                        BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);

                        //考虑配置信息的更新和删除
                        //1.当更新的时候，新数据会覆盖旧数据，不需要处理
                        //2.新增的数据，直接状态会新增数据，无需处理
                        //3.如果配置信息删除，以后这张维度表不做任何处理，那么phoenix的表也应该删除，
                        //状态中的数据应该删除

                        if("d".equals(tp.getOp())){
                              state.remove(key);//如果配置信息删除，删除状态中的信息
                              tpMap.remove(key);//把预加载的配信信息也需要删除
                        }else {
                              //把数据放入到state中
                              state.put(key,tp);

                        }

                    }
                });
                 //.print();






    }

    private SingleOutputStreamOperator<TableProcess> createTableOnPhoenix(SingleOutputStreamOperator<TableProcess> tpStream) {

        return tpStream
                .filter(tp -> "dim".equals(tp.getSinkType()))
                .map(new RichMapFunction<TableProcess, TableProcess>() {

            private Connection conn;

            //open方法和并行度相关
            //应该是每个并行度有一个连接,而不是每条数据都有一个连接
            @Override
            public void open(Configuration parameters) throws Exception {
                //1.建立到phoenix的连接
                conn = JdbcUtil.getPhoenixConnection();
            }

            @Override
            public void close() {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public TableProcess map(TableProcess tp) throws Exception {
                //根据每条配置信息,在phoenix中建表
                //在phoenix中建表全是varchar,不确定是什么类型
                //phoenix必须要有主键,主键将来是hbase中的rowkey
                //create table user(id varchar,sex varchar ,constraint pk primary key(id))
                if (conn.isClosed()) {
                    conn = JdbcUtil.getPhoenixConnection();
                }
                String op = tp.getOp();
                StringBuilder sql = new StringBuilder();
                if ("r".equals(op) || "c".equals(op)){
                    sql = getCreateTableSql(tp);
                }else if ("d".equals(op)){
                    sql = getDelTableSQL(tp);

                }else{
                    //先删除
                    StringBuilder delTableSQL = getDelTableSQL(tp);
                    PreparedStatement ps = conn.prepareStatement(delTableSQL.toString());
                    ps.execute();
                    ps.close();
                    //再创建
//                    log.warn(delTableSQL.toString());
                    sql = getCreateTableSql(tp);
                }


//                log.warn(sql.toString());
                //2.获取要给预处理语句
                PreparedStatement ps = conn.prepareStatement(sql.toString());

                //3.执行建表语句
                ps.execute();//ddl语句
//                ps.executeQuery();查询语句

                //4.关闭资源(预处理语句和连接对象)
                ps.close();

                return tp;
            }

            private StringBuilder getDelTableSQL(TableProcess tp) {

                return new StringBuilder()
                                .append("drop table if exists ")
                                .append(tp.getSinkTable());
            }

            private StringBuilder getCreateTableSql(TableProcess tp) {
                StringBuilder sql = new StringBuilder();//建表语句
                sql
                        .append(" create table if not exists  ")
                        .append(tp.getSinkTable())
                        .append("(")
                        .append(tp.getSinkColumns().replaceAll("[^,]+","$0 varchar"))
                        .append(" ,constraint pk primary key(")
                        .append(tp.getSinkPk()==null?"id":tp.getSinkPk())
                        .append("))")
                        .append(tp.getSinkExtend() == null ? "" : tp.getSinkExtend());
                return sql;
            }
        });

    }

    //    private DataStreamSink<String> readTableSource(StreamExecutionEnvironment env) {
    private SingleOutputStreamOperator<TableProcess> readTableSource(StreamExecutionEnvironment env){
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")//mysql地址
                .port(3306)//myql端口
                .databaseList("gmall_config") // set captured database
                .tableList("gmall_config.table_process") // set captured table
                .username(Constant.MYSQL_USER)
                .password(Constant.MYSQL_PASSWD)
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
                    1.都应该时json格式，格式不对是脏数据
                    2.库是gmall2022
                 */

                try {
                    JSONObject obj = JSON.parseObject(value.replaceAll("bootstrap-",""));

                    String type = obj.getString("type");
                    String data = obj.getString("data");

                    return "gmall".equals(obj.getString("database"))
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

                //2.现有数据既有事实表又有维度表，我们只要维度表的数据：过滤出需要的所有维度表数据
                //使用动态的方式过滤出想要的维度
                /**
                 * 把需要的那些维度保存到一个配置信息中，flink 可以实时读取配置信息的变化
                 * 当配置信息发生变化之后，flink程序可以不同做任何的变动，实时对配置的变化进行处理
                 * 广播状态：
                 * 把配置信息做成一个广播流，与数据流进行connect，把配置信息放入广播状态，数据信息读取
                 * 广播状态
                 * 当配置信息发生变化时，会将变化的信息广播到数据流的每个并行度中去
                 * 找一个位置存储广播流：mysql
                 * 通过maxwell实时读取到mysql中，现在使用一个新的方法：flink cdc
                 * 实现table_process表实时控制项目代码的变化
                 *只需要更新和插入的数据,由于维度表的信息基本是不变的，所以需要进行历史数据同步，需要使用boot-strap
                 * bootstrap中没有读到数据
                 * 因为我们过滤的是insert和update数据
                 *
                 *
                 *
                 */

                //3.把不同的数据写出到phoenix中的不同的表中
    }

}












































































