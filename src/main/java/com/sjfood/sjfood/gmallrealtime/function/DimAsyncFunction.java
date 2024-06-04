package com.sjfood.sjfood.gmallrealtime.function;


import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.util.DimUtil;
import com.sjfood.sjfood.gmallrealtime.util.DruidDSUtil;
import com.sjfood.sjfood.gmallrealtime.util.RedisUtil;
import com.sjfood.sjfood.gmallrealtime.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/16/9:23
 * @Package_name: com.atguigu.gmallrealtime.function
 */


public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> {

    private ThreadPoolExecutor threadPool;

    protected abstract String getTable();
    protected abstract String getTId(T input);
    protected abstract void addDim(T input, JSONObject dim);

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadPoolUtil.getThreadPool();

    }

    @Override
    // public abstract void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception;
    public void asyncInvoke( T input,  ResultFuture<T> resultFuture) {

        //多线程+多客户端
        threadPool.submit(new Runnable() {
            @Override
            public void run() {

                try {
                    //获取 phoenix 和 redis 客户端
                    Connection phoenixConn = DruidDSUtil.getPhoenixConn();
                    Jedis redisClient = RedisUtil.getRedisClient();

                    //使用 DimUtil 读取维度数据
                    JSONObject dim = DimUtil.readDim(redisClient, phoenixConn, getTable(), getTId(input));

                    //关闭连接
                    phoenixConn.close();
                    redisClient.close();

                    //补充到input中
                    addDim(input,dim);

                    //把 input 放入到ResultFuture中
                    //设置请求完成时的回调: 将结果传递给 collector
                    resultFuture.complete(Collections.singletonList(input));

                } catch (Exception e) {
                    e.printStackTrace();
                }


            }
        });


    }

}

































































