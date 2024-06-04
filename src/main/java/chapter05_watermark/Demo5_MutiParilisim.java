package chapter05_watermark;

import chapter05_watermark.pojo.MyUtil;
import com.sjfood.sjfood.gmallrealtime.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
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
 *    --------------
 *    forMonotonousTimestamps: 水印有序。
 *
 *    水印无序，提供一个乱序容忍度。
 */
public class Demo5_MutiParilisim
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
            .assignTimestampsAndWatermarks(watermarkStrategy)
            .process(new ProcessFunction<WaterSensor, WaterSensor>()
            {
                @Override
                public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                    System.out.println("下游的水印:"+ctx.timerService().currentWatermark());
                    out.collect(value);
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
