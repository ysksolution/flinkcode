import com.sjfood.sjfood.gmallrealtime.annotation.NotSink;
import com.sjfood.sjfood.gmallrealtime.bean.KeywordBean;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GetDeclaredFields {

    private static String columnName;

    public static void getColumns(StreamExecutionEnvironment tEnv ,Class<KeywordBean> tClass){

        ArrayList<String> tList = new ArrayList<>();
        Field[] declaredFields = tClass.getDeclaredFields();
        for (Field declaredField : declaredFields) {
            columnName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, declaredField.getName());
            tList.add(columnName);
        }

        String data = tList.stream().collect(Collectors.joining(","));
        System.out.println(data);

//
//        String columns = Arrays
//                .stream(tClass.getDeclaredFields())
//                .filter(f ->{
//                    // 过滤掉在 clickhouse 的表中不需要的属性
//                    // 如果这个属性有 NotSink 这个注解，就不要了
//                    return f.getAnnotation(NotSink.class) == null;
//                })
//                .map(f -> CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, f.getName())
//                )
//                .collect(Collectors.joining(","));
    }

    public static void main(String[] args) {

        StreamExecutionEnvironment tEnv = new StreamExecutionEnvironment();
        getColumns(tEnv,KeywordBean.class);
    }
}
