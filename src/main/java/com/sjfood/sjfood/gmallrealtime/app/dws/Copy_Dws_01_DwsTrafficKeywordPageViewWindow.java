package com.sjfood.sjfood.gmallrealtime.app.dws;


import com.sjfood.sjfood.gmallrealtime.annotation.NotSink;
import com.sjfood.sjfood.gmallrealtime.app.BaseSQLAPP;
import com.sjfood.sjfood.gmallrealtime.bean.KeywordBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.function.IkAnalyzer;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import com.sjfood.sjfood.gmallrealtime.util.SQLUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.Collectors;


/**
 * @Author: YSKSolution
 * @Date: 2022/11/11/20:19
 * @Package_name: com.atguigu.gmallrealtime.app.dws
 * <p>
 * 流量域搜索关键词粒度页面浏览各窗口汇总表
 * ---------
 * sql技术
 * 1. 建表，从 dwd_traffic_page 读取数据
 * 2. 查看所有的用户搜索记录，得到搜索的关键词
 * 3. 开窗聚合
 * grouped window
 * tvf （tvf是grouped window的替换，是grouped window功能提升）
 * over
 * 4. dws 的结果写到 Clickhouse 中
 * <p>
 * <p>
 * ---------------------
 * 0-5 苹果 10
 * 0-5 手机 2
 * 5-10  ...
 * 10-15 ...
 * <p>
 * 命名：dws_数据域_粒度_业务_window
 */
@Slf4j
public class Copy_Dws_01_DwsTrafficKeywordPageViewWindow extends BaseSQLAPP {


    public static void main(String[] args) {

        new Copy_Dws_01_DwsTrafficKeywordPageViewWindow().init(
                4001, 2, "Dws_01_DwsTrafficKeywordPageViewWindow"
        );

    }


    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {


        //1.建表：与 dwd_traffic_page 关联
        /*
            建表语句
            create table if not exists orders (
                `user` bigint,
                product string,
                order_time timestamp(3),
                watermark for order_time as order_time - interval '5' second
            ) with ( . . . );

         */
        String createTrafficPage = "create table if not exists dwd_traffic_page( " +
                "page map<string,string>, " +
                "ts bigint, " +
                "et as to_timestamp_ltz(ts, 3),   " +  //转换成时间戳 (ts,0/3) 0表示是秒，3表示是毫秒 如何把一个bigint转为时间戳类型
                "WATERMARK FOR et AS et - interval '3' second" + // 乱序程度是3秒
                ")" + SQLUtil.getKafkaSourceDDL(Constant.DWD_TRAFFIC_PAGE, "Copy_Dws_01_DwsTrafficKeywordPageViewWindow", "json");

        log.warn("建表語句：" + createTrafficPage);

        tEnv.executeSql(createTrafficPage);

        Table table = tEnv.sqlQuery("select * from dwd_traffic_page");

//        table.execute().print();

        tEnv.createTemporaryView("dwd_traffic_page", table);


        //2.过滤出用户搜索记录，得到关键词
        String searchSQL = "select page['item'] keyword," +
                "et " +
                "from dwd_traffic_page " +
                "where (page['last_page_id'] = 'home' or page['last_page_id'] = 'search') " +
                "and page['item_type'] = 'keyword' " +
                "and page['item'] is not null ";

        log.warn("过滤用户搜索记录" + searchSQL);

        Table keyword = tEnv.sqlQuery(searchSQL);

        tEnv.createTemporaryView("keyword_table", keyword);

//        tEnv.sqlQuery("select * from keyword").execute().print();


        //3.分词功能实现
        //自定义函数
        //3.1先注册自定义函数
        // 第一个是名字 "ik_analyzer"，第二个是函数类型  IkAnalyzer.class
        // 一进多出 ，使用TableFunction，一进一出：ScalarFunction，多进一出：AggravateFunction，多进多出：TableAggravateFunction
        tEnv.createTemporaryFunction("ik_analyzer", IkAnalyzer.class);

        //3.2 使用分词功能
        Table splitWord = tEnv.sqlQuery("select " +
                "kw,    " +
                "et " +
                "from keyword_table   " +
                "join lateral table(ik_analyzer(keyword)) as t(kw) on true");

        tEnv.createTemporaryView("splitword",splitWord);

        //4. 开窗分组聚合，每隔5s中就开一次窗
        String groupBySQL = "select " +
                "kw keyword,    "+
                "date_format(window_start,'yyyy-mm-dd hh:mm:ss') stt,  " +
                "date_format(window_end,'yyyy-mm-dd hh:mm:ss') edt,  " +
                "count(1) keywordCount,  " +
                "unix_timestamp() * 1000 ts " + // 获取现在时间
                "from table(tumble(table splitword, descriptor(et), interval '5' seconds))  " +
                "group by window_start, window_end, kw";

        log.warn("分组聚合SQL："+groupBySQL);

        Table data = tEnv.sqlQuery(groupBySQL);

        //5. 将数据写入 clickhouse 中
        // 5.1 将table转为流，然后再将流写入到 clickhouse
        SingleOutputStreamOperator<KeywordBean> dataStream = tEnv.toRetractStream(data, KeywordBean.class)
                .filter(new FilterFunction<Tuple2<Boolean, KeywordBean>>() {
                    // 只要转换成功的
                    @Override
                    public boolean filter(Tuple2<Boolean, KeywordBean> value) throws Exception {
                        return value.f0;
                    }
                })
                // 只要数据部分
                .map(new MapFunction<Tuple2<Boolean, KeywordBean>, KeywordBean>() {
                    @Override
                    public KeywordBean map(Tuple2<Boolean, KeywordBean> value) throws Exception {
                        return value.f1;
                    }
                });

//        dataStream.print("dataStream");

//        dataStream.addSink(FlinkSinKUtil.getClickhouseSink2("dws_traffic_keyword_page_view_window",KeywordBean.class));
        dataStream.addSink(getClickhouseSink2("dws_traffic_keyword_page_view_window",KeywordBean.class));
//        dataStream.addSink(FlinkSinKUtil.getClickHouseSink("dws_traffic_keyword_page_view_window",KeywordBean.class));

//        dataStream.addSink(FlinkSinKUtil.getClickHouseSink("dws_traffic_keyword_page_view_window",KeywordBean.class));
//
//        tEnv.executeSql("create table dwd_traffic_page( " +
//                //字段名
//                " page map<string,string>," +
//                " ts bigint, " +
//                " et as to_timestamp_ltz(ts,3), " + //转换成时间戳 (ts,0/3) 0表示是秒，3表示是毫秒 如何把一个bigint转为时间戳类型
//                " watermark for et as et - interval '3' second " + // 乱序程度是3秒
//                /*
//                     乱序程度如何设置？
//                     一般先设置一个值，测试这个乱序度如何
//                     比如先设置一个3s乱序度，之后用侧输出流输出迟到的数据，看迟到的数据多不多
//                     如果迟到的数据没有，说明所有数据都进入了窗口，准确但是时效性不高，把乱序程度减小一点，看还有没有迟到，慢慢调整
//                     如果迟到的数据多，说明这业务数据乱序度比较大，时效性高但是准确度不高，把乱序程度增大一点
//
//                 */
//
//                ")" + SQLUtil.getKafkaSourceDDL(Constant.DWD_TRAFFIC_PAGE, "dwd_traffic_page") +
//                "");
//
////        tEnv.sqlQuery("select * from dwd_traffic_page").execute().print();
//        tEnv.createTemporaryView("dwd_traffic_page", tEnv.sqlQuery("select * from dwd_traffic_page"));
////
////        //2.过滤出用户搜索记录，得到关键词
////        /*
////           什么样的数据是搜索记录
////            last_page_id = 'search' or 'home'
////         （这个条件可以不加，因为当 item_type = 'keyword' 时，表示当前页面是关键词页面，加了语义会更加明确，说明这个关键词是通过搜索界面得到的）
////            item_type = 'keyword'
////            item is not null
////         */
//        Table keyWord = tEnv.sqlQuery("select" +
//                " page['item'] keyword, " +
//                " et " +
//                "from dwd_traffic_page " +
//                " where (page['last_page_id'] = 'search' or page['last_page_id'] = 'home' ) " +
//                " and page['item_type'] = 'keyword' " +
//                " and page['item'] is not null " +
//                "");
////        keyWord.execute().print();
//        tEnv.createTemporaryView("keyword_table", keyWord);
//
//
//        /*
//            分词：
//            分词器 ik 对字符串按照中文的习惯进行分词
//            目前中文分词器有很多，ik算是比较好的中文分词器
//
//            A可能会这样搜索： "手机 苹果手机 256g白色苹果手机"
//            B可能会这样搜索： "手机 小米手机 256g白色小米手机"
//            C可能会这样搜索： "手机 华为手机"
//
//            直接统计A、B、C是没有意义的，因为每个人每次搜索记录是不一样的，所以需要进行分词
//            对这句话进行分词：手机 苹果手机 256g白色苹果手机
//            结果如下：
//                    手机
//                    苹果手机
//                    256g
//                    通过手机
//
//
//
//               FlinkSQL自定义函数：
//                    一进一出：ScalarFunction
//                    一进多出：TableFunction
//                    多进一出：AggravateFunction
//                    多进多出：TableAggravateFunction
//
//                分词是一进多出，使用TableFunction
//         */
//
//
//        //3.搜索记录关键词进行分词
//        //自定义函数
//        //3.1先注册自定义函数
//        // 第一个是名字 "ik_analyzer"，第二个是函数类型  IkAnalyzer.class
//        tEnv.createTemporaryFunction("ik_analyzer", IkAnalyzer.class);
//
//        //原表和制成的表通过join连接起来
//        //3.2在 sql 中使用，一进多出，炸开后的数据
//        Table resultTable = tEnv.sqlQuery("select " +
//                " kw, " +
//                " et " +
//                "from keyword_table " +
//                " join lateral table(ik_analyzer(keyword)) as t(kw) on true");
//
//        /*
//        join lateral table(ik_analyzer(keyword)) as t(kw) on true
//            这里使用到了t(kw) 和 IkAnalyzer中加的注解功能是一样的
//            @FunctionHint(output = @DataTypeHint("row<kw string>")) // 指明每行数据的列名和列类型
//         */
//
//
////        resultTable.execute().print();
//        tEnv.createTemporaryView("result_table", resultTable);
////
////        //3.开窗聚合
//        Table result = tEnv.sqlQuery("select " +
//                " kw keyword," +
//                " date_format(window_start ,'yyyy-MM-dd HH:mm:ss') stt ," +
//                " date_format(window_end ,'yyyy-MM-dd HH:mm:ss') edt, " +
//                " count(*) keyword_count ," + //count(1),count(*) sum(1) (前三个结果是一样的，有多少行，得到的值就是多少)count(id)(计算的是非空字段)
//                " UNIX_TIMESTAMP() * 1000  ts " +
//                "from table(tumble(table result_table,descriptor(et), interval '5' second))" + // 开了5秒钟的窗口
//                "group by kw, window_start,window_end " +
//                "");
//
////        result.execute().print();
//
//        //4.写出到 clickhouse 中
//    /*    写出到 clickhouse 中，首先得去 flink 官网看看是否有 clickhouse 连接器
//        发现没有
//        这个时候，我们就去看看jdbc是否可以，但是发现jdbc也不支持
//        所以就只能自定义了
//        将表数据直接写入到clickhouse中是比较困难的，所以我们需要将数据转为流数据
//        流数据比较好转
//        */
//
//        //由于没有专用的 clickhouse 连接器，所以需要自定义
//
//        //将表数据转为流的时候，表数据类型需要和bean类型完全一致
//        SingleOutputStreamOperator<KeywordBean> stream = tEnv
//                .toRetractStream(result, KeywordBean.class)
//                // 只要true：新增或者更新后的数据
//                .filter(new FilterFunction<Tuple2<Boolean, KeywordBean>>() {
//                    @Override
//                    public boolean filter(Tuple2<Boolean, KeywordBean> value) throws Exception {
//                        return value.f0;
//                    }
//                })
//                .map(new MapFunction<Tuple2<Boolean, KeywordBean>, KeywordBean>() {
//                    @Override
//                    public KeywordBean map(Tuple2<Boolean, KeywordBean> value) throws Exception {
//                        return value.f1;
//                    }
//                });
////                .map(t -> t.f1);
//
////        stream.print();
//        stream.addSink(FlinkSinKUtil.getClickHouseSink("dws_traffic_keyword_page_view_window", KeywordBean.class));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    // 六部曲
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
                                ps.setObject(position++,v);
                            }
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



    private void getColumnValue(SingleOutputStreamOperator<KeywordBean> dataStream, Class<KeywordBean> tClass) {

        dataStream.map(new MapFunction<KeywordBean, KeywordBean>() {
            @Override
            public KeywordBean map(KeywordBean value) throws Exception {

                Field[] fields = value.getClass().getDeclaredFields();
                for (Field field : fields) {
                    field.setAccessible(true);
                    String name = field.getName();
                    Object obj = field.get(value);
                    System.out.println("获取列名："+name);
                    System.out.println("获取值："+obj);
                }
                return null;
            }
        });

    }


}


