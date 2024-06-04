package chapter05_watermark;

import chapter05_watermark.pojo.MyUtil;
import com.sjfood.sjfood.gmallrealtime.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Created by Smexy on 2022/10/25
 *
 *    forMonotonousTimestamps: 水印有序。
 *          watermark =  Eventtime - 1
 *
 *    水印无序，提供一个乱序容忍度。
 *
 *    forBoundedOutOfOrderness(Duration.ofSeconds(3))：减缓水印的推进。
 *
 *           watermark =  Eventtime - 1 - Duration.ofSeconds(3)
 *
 */
public class Demo3_OrdernessWindow
{
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 3333);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //设置水印自动发生的间隔
        env.getConfig().setAutoWatermarkInterval(2000);

        //刚开始玩，先调为1
        env.setParallelism(1);

        //创建一个水印生成策略
        //水印的场景 ，可能存在乱序(迟到)，最大的乱序容忍度是3秒
        WatermarkStrategy<WaterSensor> watermarkStrategy =
            WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>()
            {
                //从T类型中抽取一个时间戳(毫秒)属性
                @Override
                public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                    return element.getTs();
                }
            });//从T类型中取什么数据作为水印*/

        env
           .socketTextStream("hadoop102", 8888)
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
           })
           //从Event中提取数据作为水印
           // [0,4999], [0,5000)
           // [5000,9999]
           .assignTimestampsAndWatermarks(watermarkStrategy)
           .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
           .process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>()
           {
               @Override
               public void process(Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

                   MyUtil.printTimeWindow(context.window());
                   String str = MyUtil.toList(elements).toString();
                   out.collect(str);

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
