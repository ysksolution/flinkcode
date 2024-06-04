package com.sjfood.sjfood.gmallrealtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.sjfood.sjfood.gmallrealtime.common.Constant;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;


import java.sql.Connection;
import java.util.List;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/15/18:42
 * @Package_name: com.sjfood.sjfood.gmallrealtime.util
 */
@Slf4j
public class DimUtil {

    public static JSONObject readDimFromPhoenix(Connection conn, String table, String id) {

        String sql = " select * from  " + table + " where id=?";

        List<JSONObject> dims = JdbcUtil.queryList(conn,
                sql,
                new String[]{id},//数组的长度是几，表示占位符的个数是几
                JSONObject.class);

        if (dims.size() == 0) {
            //表示没查到相应的维度信息
            throw new RuntimeException("没有查到对用的维度信息，请检测你的维度名和 id 的值是否正确：表名= "+ table + ",id=" + id);
        }

        return dims.get(0);//返回查到的维度信息
    }


    public static JSONObject readDim(Jedis redisClient, Connection phoenixConn, String table, String id) {

        //1.先从 redis 读取配置信息
        JSONObject dim = readDimFromRedis(redisClient,table,id);

        //2.如果读到了，则直接返回

        //3.如果没有读到，则从 phoenix 读取，然后再写入到缓存
        if(dim == null){
            log.warn("走数据：" + table + " " + id);
            dim = readDimFromPhoenix(phoenixConn,table,id);
            //然后再写入缓存
            writeDimToRedis(redisClient,table,id,dim);
        }else {
            log.warn("走缓存：" +  table + " " + id);
        }

        return dim;
    }

    private static void writeDimToRedis(Jedis redisClient, String table, String id, JSONObject dim) {

        //key: table:id
        String key = getRedisKey(table, id);

        //写入数据
        redisClient.set(key,dim.toJSONString());

        //设置ttl： two days
        redisClient.expire(key,2 * 24 * 60 * 60);

        //这是简化过程
        redisClient.setex(key, Constant.TTL_TWO_days,dim.toJSONString());

    }


    //TODO
    private static JSONObject readDimFromRedis(Jedis redisClient, String table, String id) {

        String key = getRedisKey(table, id);
        String json = redisClient.get(key);

        JSONObject dim = null;
        if (json != null) {

            dim = JSON.parseObject(json);//把字符串解析成
        }

        return dim;
    }

    public static String getRedisKey(String table, String id) {

        return table + ":" + id;
    }
}


