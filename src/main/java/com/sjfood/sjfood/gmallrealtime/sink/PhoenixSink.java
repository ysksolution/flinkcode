package com.sjfood.sjfood.gmallrealtime.sink;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.bean.TableProcess;
import com.sjfood.sjfood.gmallrealtime.util.DimUtil;
import com.sjfood.sjfood.gmallrealtime.util.DruidDSUtil;
import com.sjfood.sjfood.gmallrealtime.util.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/7/21:42
 * @Package_name: com.atguigu.gmallrealtime.sink
 */
@Slf4j
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {


    private Connection conn;
    private Jedis redisClient;


    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DruidDSUtil.getPhoenixConn();
        redisClient = RedisUtil.getRedisClient();

    }

    //流中每来一条数据，则执行依次这个方法
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> t, Context context) throws Exception {
        JSONObject data = t.f0;
        TableProcess tp = t.f1;
        //1.把维度数据通过 jdbc 的方式写入到 phoenix 中
        writeDimToPhoenix(data, tp);

        //2.更新或者删除 redis 中缓存的数据
        delCache(data,tp);
    }

    private void delCache(JSONObject data, TableProcess tp) {


        //什么时候需要删除缓存中的数据？？
        //当这次的维度数据是 update

        String opType = data.getString("op_type");
        if ("update".equals(opType)) {

            String key = DimUtil.getRedisKey(tp.getSinkTable(), data.getString("id"));

            //删除key
            //redis中删除一个不存在的 key 会怎么样？
            //不会有异常

            redisClient.del(key);
        }


    }

    private void writeDimToPhoenix(JSONObject data, TableProcess tp) throws SQLException {
        //实现写入业务
        //jdbc：执行一个插入语句
        //向phoenix插入sql语句
        //upsert into 表名（a,b,c)values(?,?,?)
        StringBuilder sql = new StringBuilder();
        // 拼接sql
        sql
                .append("upsert into ")
                .append(tp.getSinkTable())
                .append("(")
                .append(tp.getSinkColumns())
                .append(") values(")
                .append(tp.getSinkColumns().replaceAll("[^,]+","?"))
                .append(")");
        log.warn("插入语句：" + sql.toString());
        PreparedStatement ps = conn.prepareStatement(sql.toString());

        // 给占位符进行赋值

        //根据列名去data中获取数据
        String[] columns = tp.getSinkColumns().split(",");

        for (int i = 0; i < columns.length; i++) {
            String columnName= columns[i];
            //phoenix 表中的所有字段都是varchar
            String v = data.getString(columnName);
            ps.setString(i + 1,v);

        }
        ps.execute();
        //phoenix不可以自动提交,得手动提交
        conn.commit();
        ps.close();
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            //1.如果连接对象是直接获取，则是关闭连接
            //2.如果连接是从连接池获取，则是归还连接
            conn.close();
        }

        if (redisClient != null) {
            redisClient.close();
        }

    }

/*
长连接问题：
    mysql：当一个连接超过八个小时与mysql服务器没有通讯，则服务器会自动关闭服务
解决长连接问题：
    1.每隔一段时间，与服务器做一次通讯 select 1;
    2.使用前，先判断这个连接是否还在连接，如果没有，重新
    获取一个新的连接
    3.使用连接池
        连接池会避免连接关闭
        druid 德鲁伊
*/


}
