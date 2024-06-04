package com.sjfood.sjfood.gmallrealtime.app.dwd.db;


import com.sjfood.sjfood.gmallrealtime.app.BaseSQLAPP;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/8/8:55
 * @Package_name: com.atguigu.gmallrealtime.app.dwd.db
 */
public class Dwd_02_DwdTradeCartAdd extends BaseSQLAPP
{

    public static void main(String[] args) {

        new Dwd_02_DwdTradeCartAdd().init(
                62000,
                2,
                "DwdTradeCartAdd"
              );
    }



    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {


        //1.通过ddl建立动态表与ods_db关联，从这个topic进行读取数据
        readOdsDb(tEnv,"Dwd_02_DwdTradeCartAdd");

        //2.读取字典表的数据
        readBaseDic(tEnv);

        //3.过滤出 cart_info 的数据
        String sql ="select " +
                "  `data`['id'] id, " +
                "  `data`['user_id'] user_id, " +
                "  `data`['sku_id'] sku_id, " +
                "  `data`['cart_price'] cart_price, " +
                " if(`type`='insert',"+
                "  cast(`data`['sku_num'] as int)," +
                " cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) " +
                ") sku_num, " +
                " `data`['source_id'] source_id, "+
                " `data`['source_type'] source_type, "+
                " ts ,"+
                " pt "+
                " from ods_db " +
                " where `database` = 'gmall2022' " +
                " and `table` = 'cart_info' " +
                " and (" +
                " `type` = 'insert' " +
                " or (`type`='update' " +
                " and `old`['sku_num'] is not null" +
                " and cast(`data`['sku_num'] as int) >  cast(`old`['sku_num'] as int)"+
                "))";
//        System.out.println(sql);
        Table cartInfo = tEnv.sqlQuery(sql);
        tEnv.createTemporaryView("cart_info",cartInfo);

        //cart_info lookup join 字典表
        Table result = tEnv.sqlQuery(" select " +
                "  ci.id, " +
                "  ci.user_id," +
                " ci.sku_id," +
                " ci.source_id, " +
                " ci.source_type, " +
                " dic.dic_name source_type_name, " +
                " ci.sku_num, " +
                "ci.ts " +
                " from cart_info ci " +
                " join base_dic for system_time as of ci.pt as dic " +
                " on ci.source_type = dic.dic_code");
//        result.execute().print();


        //3.把加购事实表数据写入到kafka中
        tEnv.executeSql(" create table dwd_trade_cart_add(" +
                "id string," +
                "user_id string," +
                "sku_id string," +
                "source_id string," +
                "source_type_code string," +
                "source_type_name string," +
                "sku_num int," +
                "ts string" +
                ") " + SQLUtil.getKafkaSinkDDL(Constant.DWD_TRADE_CART_ADD,"json"));
        result.executeInsert("dwd_trade_cart_add");
    }


}
/*
 * 过滤加购数据：新增和更新
 * 如果是更新的话
 *      sku_num 变化，变大
 *      old[sku_num] is not null sku_num 发生了变化
 *      data[sku_num] > old[sku_num]
 *      id sku_num
 *      1    2   新增 ->2
 *      1    6   新增->6
 *      select sum(sku_num) from a group by sku_id
 *      需要将data[sku_num] - old[sku_num] 为新增的加购数量
 *
 */

/*
 * 加购事务事实表
 * 数据源：从ods_db里面的cart_info
 * 业务：
 *  新增的加购
 *  变化
 *      只关注 sku_num 增加的情况
 *      减少，删除动作不考虑，因为今天删除，减少，昨天的数据已经算完了，结果已经统计出来了，
 * 最后写入到 kafka 中
 * 流的方式处理还是 sql 的方式
 * 原则：越简单越好
 * sql:业务数据，存储在mysql中，mysql中的数据是结构化的数据
 * sql非常擅长处理结构化的数据
 * 流：处理非结构化，半结构化数据，流比较灵活
 *
 *
 */
























