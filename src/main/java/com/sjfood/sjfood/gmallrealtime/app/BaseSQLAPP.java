package com.sjfood.sjfood.gmallrealtime.app;


import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @Author: YSKSolution
 * @Date: 2022/11/4/14:06
 * @Package_name: com.atguigu.gmallrealtime.app
 */
public abstract class BaseSQLAPP {

    public void init(int port, int p, String ckAndJobName){

        //从kafka中读取topic ods_db 数据
        System.setProperty("HADOOP_USER_NAME","sarah");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);

        //1.开启ck
        env.enableCheckpointing(3000);
        //2.设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        //3.设置ck的存储路径
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop103:8020/gmall/" + ckAndJobName);
        //4.设置ck的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        //5.设置ck模式：严格一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //6.设置ck的并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //7.设置ck之间的时间最小间隔,如果设置了这个，6可以省略，6.7二选一
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //8.设置当job取消的时候，是否保留ck的数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //9.给sql job设置name

        //DDL
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().getConfiguration().setString("pipeline.name",ckAndJobName);
        handle(env,tEnv);
    }

    public void readBaseDic(StreamTableEnvironment tEnv){
        tEnv.executeSql("create table base_dic(" +
                "dic_code string," +
                "dic_name string" +
                ")with(" +
                " 'connector' = 'jdbc', " +
                " 'url' = 'jdbc:mysql://hadoop102:3306/gmall?useSSL=false', " +
                " 'table-name' = 'base_dic', " +
                " 'username' = 'root', " +
                " 'password' = 'mivbAs7Awc' ," +
                //时间设置多长合适？
                //在性能和准确性做一个平衡
                " 'lookup.cache.ttl' = '1 hour', " +
                " 'lookup.cache.max-rows' = '10' " +
                ")") ;
    }

    protected void readOdsDb(StreamTableEnvironment tEnv, String groupId) {
        tEnv.executeSql("create table if not exists ods_db(" +
                " `database` string,"+
                "  `table` string, "+
                "  `type` string, "+
                "  `ts` string," +
                "  `data` map<string,string>," +
                "  `old` map<string,string> ," +
                " pt as proctime() "+
                ") " + SQLUtil.getKafkaSourceDDL(Constant.TOPIC_ODS_DB,groupId,"json"));
//        tEnv.sqlQuery("select * from ods_db").execute().print();
    }





    protected abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv);


}
