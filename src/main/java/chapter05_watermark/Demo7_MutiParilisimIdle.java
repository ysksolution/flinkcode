package chapter05_watermark;

import chapter05_watermark.pojo.MyUtil;
import com.sjfood.sjfood.gmallrealtime.bean.WaterSensor;;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
     模拟上游由于数据倾斜，导致某些并行度无法收到新数据，水印无法推进。
    下游根据木桶原理，取上游水印最小的，导致下游的水印也无法推进，窗口无法计算。

 */
public class Demo7_MutiParilisimIdle
{
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 3334);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);


        //设置水印自动发生的间隔
        env.getConfig().setAutoWatermarkInterval(2000);

        //全局2
        env.setParallelism(2);

        //创建一个水印生成策略
        //水印的场景 连续有序(目前)，偶尔乱序
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
            //把系统的时钟调慢3s
            .<WaterSensor>forMonotonousTimestamps()
            .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>()
            {
                //从T类型中抽取一个时间戳(毫秒)属性
                @Override
                public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                    return element.getTs();
                }
            })
            // idle: 赋闲   15秒内，没有数据产生，就丧失发送水印的权利，赋闲在家
            .withIdleness(Duration.ofSeconds(15));//从T类型中取什么数据作为水印*/

       env
            .socketTextStream("hadoop103", 8888)
            .global()
            .map(new MapFunction<String, WaterSensor>()
            {
                @Override
                public WaterSensor map(String value) throws Exception {
                    String[] data = value.split(",");
                    return new WaterSensor(
                        data[0],
                        Long.valueOf(data[1]),
                        Integer.valueOf(data[2])
                    );
                }
            }).setParallelism(6)
            //从Event中提取数据作为水印
            // [0,4999], [0,5000)
            .assignTimestampsAndWatermarks(watermarkStrategy)
            .keyBy(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>()
            {
                //窗口中，process是一个窗口在计算时被触发
                @Override
                public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                    System.out.println(context.window());
                    out.collect(MyUtil.toList(elements).toString());
                }
            })
            .print();




        try {
                    env.execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }

    }
}
