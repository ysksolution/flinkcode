package com.sjfood.sjfood.gmallrealtime.function;


import com.sjfood.sjfood.gmallrealtime.util.IKSpllit;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;

import java.util.List;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/11/21:08
 * @Package_name: com.atguigu.gmallrealtime.function
 */


//@FunctionHint(output = @DataTypeHint("row<kw string>")) // 指明每行数据的列名和列类型
//TableFunction <T> The type of the output row  ，TableFunction有个泛型T，表示输出的每行类型，因为炸开后的数据是一行一行的，指明每行类型
public class IkAnalyzer extends TableFunction<String> {
    /*
      public abstract class TableFunction<T> extends UserDefinedFunction
     @param <T> The type of the output row. Either an explicit composite type or an atomic type that
    is implicitly wrapped into a row consisting of one field.

     */
    // 一进多出，通过collect丢出去
    // 这个方法必须实现
    public void eval(String keyword){
        //我是中国人
        List<String> kws = IKSpllit.split(keyword);

        for (String kw : kws) {
            collect(kw);
        }

    }
}
