package com.sjfood.sjfood.gmallrealtime.util;

import java.sql.*;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/16/21:15
 * @Package_name: com.atguigu.gmallrealtime.util
 */
public class PheonixConnection {


    public static void main(String[] args) throws SQLException {

        String url = " jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
        //1.phoenix jdbc 操作
        //1.获取连接
        Connection conn = DriverManager.getConnection(url);

        //预编译
        PreparedStatement ps = conn.prepareStatement("select * from student");
        //执行查询
        ResultSet resultSet = ps.executeQuery();

        //输出结果
        while (resultSet.next()) {
           ;;
        }

        if (conn != null) {
            conn.close();
        }

    }
}
