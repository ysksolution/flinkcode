package com.sjfood.sjfood.gmallrealtime.app;

import com.sjfood.sjfood.gmallrealtime.util.FileSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Author: YSKSolution
 * @Date: 2022/11/4/14:06
 * @Package_name: com.atguigu.gmallrealtime.app
 */
public abstract class BaseAPPV1 {

    public void init(int port, int p, String ckAndGroupIdAndJobName, String topic){

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
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop103:8020/gmall/" + ckAndGroupIdAndJobName);
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


        //从topic中读取数据
        DataStreamSource<String> stream = env.addSource(FileSourceUtil.getKafkaSource(ckAndGroupIdAndJobName, topic));
//        stream.print();
        handle(env,stream);
        //根据得到的流，完成业务逻辑


        try {
            env.execute(ckAndGroupIdAndJobName);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);

}
