package com.sjfood.sjfood.gmallrealtime.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/16/9:03
 * @Package_name: com.atguigu.gmallrealtime.util
 */


public class ThreadPoolUtil {


    public static ThreadPoolExecutor getThreadPool(){


        return new ThreadPoolExecutor(
                200, //线程池核心线程数
                500,//最大线程数
                60,//空闲线程存活时间
                TimeUnit.SECONDS,//时间单位
                new LinkedBlockingQueue<>()

        );
    }

}
/**
 * new Thread() （覆写 run 方法）.start
 * new Thread(new Runnable).start()
 */










































