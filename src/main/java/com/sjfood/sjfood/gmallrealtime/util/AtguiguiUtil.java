package com.sjfood.sjfood.gmallrealtime.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/10/9:51
 * @Package_name: com.atguigu.gmallrealtime.util
 */
public class AtguiguiUtil {
    public static String tsToDate(Long ts) {

        return new SimpleDateFormat("yyyy-MM-dd").format(ts);


    }

    public static String tsToDateTime(long ts) {

        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);

    }

    public static Long dataToTs(String date) throws ParseException {


        return new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime();

    }
}
