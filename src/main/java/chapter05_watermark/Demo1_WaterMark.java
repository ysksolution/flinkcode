package chapter05_watermark;


import com.sjfood.sjfood.gmallrealtime.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Created by Smexy on 2022/10/25
 *
 *      水印的价值在于和EventTime窗口一起使用。
 *          水印不和窗口用也行，但是没意义。
 *
 *          水印从哪里来?  从Event中提取
 */
public class Demo1_WaterMark
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
        //水印的场景 连续有序(目前)，偶尔乱序
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
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
           .assignTimestampsAndWatermarks(watermarkStrategy)
           .process(new ProcessFunction<WaterSensor, String>()
           {
               //一条数据调用一次
               //processElement： 只处理数据，不处理水印
               @Override
               public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                   TimerService timerService = ctx.timerService();
                   //timerService.currentWatermark()： 当有一条数据到达Process调用 processElement时，时间是多少！
                   System.out.println("主观时间当前watermark：" +timerService.currentWatermark());
                   System.out.println("客观时间:"+timerService.currentProcessingTime());
                   out.collect(value.toString());
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
