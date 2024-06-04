package com.sjfood.sjfood.gmallrealtime.app.dws;


import com.sjfood.sjfood.gmallrealtime.app.BaseSQLAPP;
import com.sjfood.sjfood.gmallrealtime.bean.KeywordBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.function.IkAnalyzer;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import com.sjfood.sjfood.gmallrealtime.util.SQLUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/11/20:19
 * @Package_name: com.atguigu.gmallrealtime.app.dws
 *
 * 流量域搜索关键词粒度页面浏览各窗口汇总表
 * ---------
 * sql技术
 * 1. 建表，从 dwd_traffic_page 读取数据
 * 2. 查看所有的用户搜索记录，得到搜索的关键词
 * 3. 开窗聚合
 *      grouped window
 *      tvf （tvf是grouped window的替换，是grouped window功能提升）
 *      over
 * 4. dws 的结果写到 Clickhouse 中
 *
 *
 * ---------------------
 * 0-5 苹果 10
 * 0-5 手机 2
 * 5-10  ...
 * 10-15 ...
 *
 * 命名：dws_数据域_粒度_业务_window
 *
 *
 */
public class Dws_01_DwsTrafficKeywordPageViewWindow extends BaseSQLAPP {


    public static void main(String[] args) {

        new Dws_01_DwsTrafficKeywordPageViewWindow().init(
                4001, 2, "Dws_01_DwsTrafficKeywordPageViewWindow"
        );

    }


    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {


        //1.建表：与dwd_traffic_page关联
        tEnv.executeSql("create table dwd_traffic_page( " +
                //字段名
                " page map<string,string>," +
                " ts bigint, " +
                " et as to_timestamp_ltz(ts,3), " + //转换成时间戳 (ts,0/3) 0表示是秒，3表示是毫秒 如何把一个bigint转为时间戳类型
                " watermark for et as et - interval '3' second " + // 乱序程度是3秒
                /*
                     乱序程度如何设置？
                     一般先设置一个值，测试这个乱序度如何
                     比如先设置一个3s乱序度，之后用侧输出流输出迟到的数据，看迟到的数据多不多
                     如果迟到的数据没有，说明所有数据都进入了窗口，准确但是时效性不高，把乱序程度减小一点，看还有没有迟到，慢慢调整
                     如果迟到的数据多，说明这业务数据乱序度比较大，时效性高但是准确度不高，把乱序程度增大一点

                 */

                ")" + SQLUtil.getKafkaSourceDDL(Constant.DWD_TRAFFIC_PAGE, "dwd_traffic_page") +
                "");

//        tEnv.sqlQuery("select * from dwd_traffic_page").execute().print();
        tEnv.createTemporaryView("dwd_traffic_page", tEnv.sqlQuery("select * from dwd_traffic_page"));
//
//        //2.过滤出用户搜索记录，得到关键词
//        /*
//           什么样的数据是搜索记录
//            last_page_id = 'search' or 'home'
//         （这个条件可以不加，因为当 item_type = 'keyword' 时，表示当前页面是关键词页面，加了语义会更加明确，说明这个关键词是通过搜索界面得到的）
//            item_type = 'keyword'
//            item is not null
//         */
        Table keyWord = tEnv.sqlQuery("select" +
                " page['item'] keyword, " +
                " et " +
                "from dwd_traffic_page " +
                " where ( page['last_page_id'] = 'search' or page['last_page_id'] = 'home' ) " +
                " and page['item_type'] = 'keyword' " +
                " and page['item'] is not null " +
                "");
//        keyWord.execute().print();
        tEnv.createTemporaryView("keyword_table", keyWord);


        /*
            分词：
            分词器 ik 对字符串按照中文的习惯进行分词
            目前中文分词器有很多，ik算是比较好的中文分词器

            A可能会这样搜索： "手机 苹果手机 256g白色苹果手机"
            B可能会这样搜索： "手机 小米手机 256g白色小米手机"
            C可能会这样搜索： "手机 华为手机"

            直接统计A、B、C是没有意义的，因为每个人每次搜索记录是不一样的，所以需要进行分词
            对这句话进行分词：手机 苹果手机 256g白色苹果手机
            结果如下：
                    手机
                    苹果手机
                    256g
                    通过手机



               FlinkSQL自定义函数：
                    一进一出：ScalarFunction
                    一进多出：TableFunction
                    多进一出：AggravateFunction
                    多进多出：TableAggravateFunction

                分词是一进多出，使用TableFunction
         */


        //3.搜索记录关键词进行分词
        //自定义函数
        //3.1先注册自定义函数
        // 第一个是名字 "ik_analyzer"，第二个是函数类型  IkAnalyzer.class
        tEnv.createTemporaryFunction("ik_analyzer", IkAnalyzer.class);

        //原表和制成的表通过join连接起来
        //3.2在 sql 中使用，一进多出，炸开后的数据
        Table resultTable = tEnv.sqlQuery("select " +
                " kw, " +
                " et " +
                "from keyword_table " +
                " join lateral table(ik_analyzer(keyword)) as t(kw) on true");

        /*
        join lateral table(ik_analyzer(keyword)) as t(kw) on true
            这里使用到了t(kw) 和 IkAnalyzer中加的注解功能是一样的
            @FunctionHint(output = @DataTypeHint("row<kw string>")) // 指明每行数据的列名和列类型
         */


//        resultTable.execute().print();
        tEnv.createTemporaryView("result_table",resultTable);
//
//        //3.开窗聚合
        Table result = tEnv.sqlQuery("select " +
                " kw keyword," +
                " date_format(window_start ,'yyyy-MM-dd HH:mm:ss') stt ," +
                " date_format(window_end ,'yyyy-MM-dd HH:mm:ss') edt, " +
                " count(*) keyword_count ," + //count(1),count(*) sum(1) (前三个结果是一样的，有多少行，得到的值就是多少)count(id)(计算的是非空字段)
                " UNIX_TIMESTAMP() * 1000  ts " +
                "from table(tumble(table result_table,descriptor(et), interval '5' second))" + // 开了5秒钟的窗口
                "group by kw, window_start,window_end " +
                "");

//        result.execute().print();

        //4.写出到 clickhouse 中
    /*    写出到 clickhouse 中，首先得去 flink 官网看看是否有 clickhouse 连接器
        发现没有
        这个时候，我们就去看看jdbc是否可以，但是发现jdbc也不支持
        所以就只能自定义了
        将表数据直接写入到clickhouse中是比较困难的，所以我们需要将数据转为流数据
        流数据比较好转
        */

        //由于没有专用的 clickhouse 连接器，所以需要自定义

        //将表数据转为流的时候，表数据类型需要和bean类型完全一致
        SingleOutputStreamOperator<KeywordBean> stream = tEnv
                .toRetractStream(result, KeywordBean.class)
                // 只要true：新增或者更新后的数据
                .filter(new FilterFunction<Tuple2<Boolean, KeywordBean>>() {
                    @Override
                    public boolean filter(Tuple2<Boolean, KeywordBean> value) throws Exception {
                        return value.f0;
                    }
                })
                .map(new MapFunction<Tuple2<Boolean, KeywordBean>, KeywordBean>() {
                    @Override
                    public KeywordBean map(Tuple2<Boolean, KeywordBean> value) throws Exception {
                        return value.f1;
                    }
                });
//                .map(t -> t.f1);

//        stream.print();
        stream.addSink(FlinkSinKUtil.getClickHouseSink("dws_traffic_keyword_page_view_window",KeywordBean.class));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}


