package chapter05_watermark;

import chapter05_watermark.pojo.MyUtil;
import com.sjfood.sjfood.gmallrealtime.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Created by Smexy on 2022/10/25
 *
 *      水印的价值在于和EventTime窗口一起使用。
 *          水印不和窗口用也行，但是没意义。
 *
 *          水印从哪里来?  从Event中提取
 *    --------------
 *    forMonotonousTimestamps: 水印有序。
 *
 *    水印无序，提供一个乱序容忍度。
 *
 *
 *    当 watermark - 1 - 乱序容忍度 >= 窗口end ，触发窗口执行
 *
 *
 * 50001 - 1 - 3000 - 3000 = 43999 当数据时间时间小于 44999 时，就不再接受该数据了
 *
 *    当  watermark - 1 - 乱序容忍度- 允许迟到时间
 *   50001
 *
 */
public class Demo4_HandleLateData
{
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 3334);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        OutputTag<WaterSensor> lateWS = new OutputTag<>("lateWS", TypeInformation.of(WaterSensor.class));

        //设置水印自动发生的间隔
        env.getConfig().setAutoWatermarkInterval(2000);

        //刚开始玩，先调为1
        env.setParallelism(1);

        //创建一个水印生成策略
        //水印的场景 连续有序(目前)，偶尔乱序
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
            //把系统的时钟调慢3s
            .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>()
            {
                //从T类型中抽取一个时间戳(毫秒)属性
                @Override
                public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                    return element.getTs();
                }
            });//从T类型中取什么数据作为水印*/

        SingleOutputStreamOperator<String> ds = env
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
            // [0,4999]: 7999会关闭

            /*
                       [0,5000),  [5000,10000) , [10000,15000),
                        10           8000
                       -> 4999       ->9999

                9999-3000-3000=3999

                b.设置allowedLateness情况下，某条数据属于某个窗口，但是watermark超过了窗口的结束时间+延迟时间，则该条数据会被丢弃；
                9999 > 5000+3000 丢弃
                9999 < 10000+3000 不丢弃

                12999  9999+3000 进不来

             */


            .assignTimestampsAndWatermarks(watermarkStrategy)
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            //允许迟到，就是窗口到底触发运行，但是不会关闭，为那些迟到的人留下一丝机会
            .allowedLateness(Time.seconds(3))
            //针对窗口关闭的数据，设置使用侧流接收
            .sideOutputLateData(lateWS)
            .process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>()
            {
                @Override
                public void process(Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

                    MyUtil.printTimeWindow(context.window());
                    String str = MyUtil.toList(elements).toString();
                    out.collect(str);

                }
            });

        //主流输出
        ds
           .print("主流");
        ds.getSideOutput(lateWS).print("侧流");


        try {
                    env.execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }

    }
}

