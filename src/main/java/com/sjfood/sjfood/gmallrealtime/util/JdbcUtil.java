package com.sjfood.sjfood.gmallrealtime.util;

import com.sjfood.sjfood.gmallrealtime.bean.TableProcess;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;
;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/7/15:31
 * @Package_name: com.sjfood.sjfood.gmallrealtime.util
 */
public class JdbcUtil {


    public static Connection getPhoenixConnection() {

        String driver = Constant.PHOENIX_DRIVER;
        String url = Constant.PHOENIX_URL;
        return getJdbcConnection(driver, url,null,null);
    }

    public static Connection getJdbcConnection(String driver,
                                               String url,
                                               String user,
                                               String password) {
        //1.加载驱动
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFoundException");
           throw new RuntimeException(e);
        }
        //2.获取连接
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            System.out.println("SQLException");
            throw new RuntimeException(e);
        }
    }

    public static Connection getMySQLConnection() {

        String driver = Constant.MYSQL_DRIVER;
        String url = Constant.MYSQL_URL;
        return getJdbcConnection(driver,url,Constant.MYSQL_USER,Constant.MYSQL_PASSWD);
    }

    //<T>List<T>
    //第一个T是声明泛型，第二是T是使用泛型，<T>List<T>在返回时声明泛型，泛型的类型是T
    //泛型可以在两个地方定义，一个是类上，在整个类中任何地方都可以使用：，一个是方法上，只能在方法中
    //用到了泛型，就必须使用反射，使用到反射，就必须使用Class对象Class<T> tClass
    public static <T>List<T> queryList(Connection conn,
                                       String querySQL,
                                       Class<T> tClass,
                                       boolean ... isToCamel)  {
        //默认下划线不转驼峰
        boolean flag = false;
        if (isToCamel.length>0) {//如果有传入，则应用传入的值
            flag = isToCamel[0];
        }

        //执行sql语句，将查询的结果封装到List集合
        ArrayList<T> list = new ArrayList<>();

        //预处理
        //在try中执行ps，会自动关闭ps，无需我们动手
        try(PreparedStatement ps = conn.prepareStatement(querySQL)) {

            //执行sql
            ResultSet resultSet = ps.executeQuery();
            //获取每一行有多少列
            ResultSetMetaData metaData = resultSet.getMetaData();
            //将结果封装到sql
            while (resultSet.next()) {
              //表示遍历到了一行
                //这一行有很多的列，把每行数据封装到一个对象T中，每列就是T中的一个属性
                T t = tClass.newInstance();//调用无参构造器创建对象
                //在java中，只要创建对象，就一定调用了构造器，
                //每一行有多少行？
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    //列名：T的属性名
                    //列值：T的属性值
                    //getColumnName:获取的是原始列名，getColumnLabel获取更新后的列名
                    String name = metaData.getColumnLabel(i);
                    if (flag) {
                        //把name转成驼峰命名
                        //a_b aB
                         name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);
                    }
                    Object obj = resultSet.getObject(i);

                    BeanUtils.setProperty(t,name,obj);

                }

                list.add(t);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


        return list;


    }


    public static <T>List<T> queryList(Connection conn,
                                       String sql,
                                       Object[] args, //给占位符赋值
                                       Class<T> tClass,
                                       boolean... isToCamel) {

        boolean flag = false;//默认下划线不转驼峰
        if (isToCamel.length > 0) {
            flag = isToCamel[0];
        }

        ArrayList<T> list = new ArrayList<>();

        try(PreparedStatement ps = conn.prepareStatement(sql)) {
            //给占位符赋值
            for (int i = 0; args != null && i < args.length; i++) {

                Object arg = args[i];
                ps.setObject(i + 1, arg);
            }

            ResultSet resultSet = ps.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                // 表示已经遍历到一行
                // 这个一行会有很多列, 把每列数据封装到一个对象中 T, 每列就是 T 中的一个属性
                T t = tClass.newInstance();  // 调用无参构造器创建对象
                // 每一行有多少列?
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    // 列名: T 的属性名
                    // 列值: T 的属性的值
                    String name = metaData.getColumnLabel(i);
                    if (flag) {
                        // 把 name 转成驼峰命名
                        // a_b aB
                        name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);
                    }
                    Object obj = resultSet.getObject(i);
                    // t.name=obj
                    BeanUtils.setProperty(t, name, obj);
                }

                list.add(t);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


        return list;

    }


    public static void main(String[] args) {

        List<TableProcess> list = queryList(getMySQLConnection(), "select * from gmall_config.table_process", TableProcess.class,true);
        for (TableProcess object : list) {
            System.out.println(object);
        }

        //从MySQL中读取

    }

    }