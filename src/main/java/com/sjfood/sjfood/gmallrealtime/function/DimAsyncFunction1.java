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
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/17/11:09
 * @Package_name: com.sjfood.sjfood.gmallrealtime.function
 */
public abstract class DimAsyncFunction1<T> extends RichAsyncFunction<T,T> {


    private ThreadPoolExecutor threadPool;

    protected abstract String getTable();
    protected abstract String getId(T input);
    protected abstract void addDim(T input, JSONObject dims);


    @Override
    public void open(Configuration parameters) throws Exception {

        threadPool = ThreadPoolUtil.getThreadPool();

    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        //多线程+多客户端
        threadPool.submit(new Runnable() {


            @Override
            public void run() {

                try {

                    //获取 redis 客户端 和 phoenix 客户端
                    Jedis redisClient = RedisUtil.getRedisClient();
                    Connection phoenixConn = DruidDSUtil.getPhoenixConn();

                    //使用 DimUtil 读取维度数据
                    JSONObject dims = DimUtil.readDim(redisClient, phoenixConn, getTable(), getId(input));

                    redisClient.close();
                    phoenixConn.close();

                    //补充到 input 中
                    addDim(input,dims);

                    //把 input 放入到 resultFuture
                    //只放一个，只需要放一个集合进去
                    resultFuture.complete(Collections.singletonList(input));

                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        });
    }
}
