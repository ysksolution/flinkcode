package com.sjfood.sjfood.gmallrealtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/11/21:16
 * @Package_name: com.atguigu.gmallrealtime.util
 */
public class IKSpllit {

    public static void main(String[] args) {
        System.out.println(split("我是中国人"));
    }


    public static List<String> split(String keyword) {

        //使用IK分词器对传入的字符串进行分词

        //如何把String -> Reader
        //文件流：将文件转为流，很方便
        // new FileReader("文件路径");可以先写入文件，再从文件中读取，台曲线救国了
        //内存流
        StringReader reader = new StringReader(keyword);
        /*
           userSmart：是否使用智能
         * 非智能分词：细粒度输出所有可能的切分结果，maxword 最多分词
         * 智能分词： 合并数词和量词，对分词结果进行歧义判断
         */
        IKSegmenter seg = new IKSegmenter(reader, true);

        /*
            为什么要使用HashSet，为了去重
            因为A可能搜索："手机 华为手机 华为256g手机"
            分词结果是：
                手机
                华为
                手机
                华为
                256g
                手机
            一次搜索，得到多个手机关键词，不适合，只要一个"手机"就可以了

         */
        HashSet<String> set = new HashSet<>();

        /*
             seg.next(）这个方法调用得到切开的词，一次得到一个
             比如 "我是中国人"，seg.next(）第一次调用得到 "我"，第二次得到是
         */

        try {
            Lexeme next = seg.next();
            while (next != null) {
                //拿到 next 中的数据
                String s = next.getLexemeText();
                set.add(s);
                next = seg.next();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>(set);
    }
}

