package com.sjfood.sjfood.gmallrealtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/15/19:51
 * @Package_name: com.atguigu.gmallrealtime.util
 */
public class RedisUtil {

    private static final JedisPool pool;

    static {

        JedisPoolConfig config = new JedisPoolConfig();

        config.setMaxTotal(300);//连接池最多提供 100 个连接
        config.setMaxIdle(10); //最多提供 10 个空闲
        config.setMinIdle(2);
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setMaxWaitMillis(10 * 1000);//当连接池没有空闲的连接的时候，等待的时间

        pool = new JedisPool(config, "hadoop102", 6379);
    }


    public static Jedis getRedisClient(){

        Jedis jedis = pool.getResource();
        jedis.select(4);
        return jedis;
    }
}
