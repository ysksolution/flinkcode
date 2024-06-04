import com.alibaba.fastjson.JSON;
import com.sjfood.sjfood.gmallrealtime.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * Created by Smexy on 2022/10/24
 *
 *  额外引入
 *          RedisSink(FlinkJedisConfigBase flinkJedisConfigBase,   //配置，配置如何去连接Redis
 *                   RedisMapper<IN> redisSinkMapper   //命令
 *                   )
 *
 *      -----------------
 *      写入hash,sorted set （zset）
 *      RedisCommandDescription：(RedisCommand redisCommand, String additionalKey)
 *
 *      其他类型: string,list,set
 *      RedisCommandDescription：(RedisCommand redisCommand)
 */
public class Demo23_RedisSink
{
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建 FlinkJedisPoolConfig
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
            .setHost("hadoop102")
            .setPort(6379)
            .setDatabase(0)
            .setMaxTotal(20)
            .setMaxIdle(10)
            .setMinIdle(5)
            .setTimeout(60000)
            .build();


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
           .addSink(new RedisSink<WaterSensor>(jedisPoolConfig,new MyZSetMapper()));

        try {
                    env.execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }

    }

    private static class  MyZSetMapper implements RedisMapper<WaterSensor>{


        // key: { score:member,score:member   }
        //描述要使用的写入redis的命令
        @Override
        public RedisCommandDescription getCommandDescription() {
            //additionalKey: 外层的key
            return new RedisCommandDescription(RedisCommand.ZADD,"wss");
        }

        // 使用数据的那部分作为member
        @Override
        public String getKeyFromData(WaterSensor data) {
            return JSON.toJSONString(data);
        }

        // 使用数据的那部分作为score
        @Override
        public String getValueFromData(WaterSensor data) {
            return data.getVc().toString();
        }
    }

    //hash
    private static class  MyHashMapper implements RedisMapper<WaterSensor>{


        // Key: { field:value,field:value  }
        //描述要使用的写入redis的命令
        @Override
        public RedisCommandDescription getCommandDescription() {
            //additionalKey: 外层的key
            return new RedisCommandDescription(RedisCommand.HSET,"sensors");
        }

        // 使用数据的那部分作为field
        @Override
        public String getKeyFromData(WaterSensor data) {
            return data.getId();
        }

        // 使用数据的那部分作为value
        @Override
        public String getValueFromData(WaterSensor data) {
            return JSON.toJSONString(data);
        }
    }


    //string类型
    private static class  MyStringMapper implements RedisMapper<WaterSensor>{


        //描述要使用的写入redis的命令
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        // 使用数据的那部分作为key
        @Override
        public String getKeyFromData(WaterSensor data) {
            return data.getId();
        }

        // 使用数据的那部分作为value
        @Override
        public String getValueFromData(WaterSensor data) {
            return JSON.toJSONString(data);
        }
    }

    //list类型
    private static class  MyListMapper implements RedisMapper<WaterSensor>{


        //描述要使用的写入redis的命令
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        // 使用数据的那部分作为key 一种传感器是一组
         @Override
        public String getKeyFromData(WaterSensor data) {
            return "list:"+data.getId();
        }

        // 使用数据的那部分作为value
        @Override
        public String getValueFromData(WaterSensor data) {
            return JSON.toJSONString(data);
        }
    }

    //set类型
    private static class  MySetMapper implements RedisMapper<WaterSensor>{


        //描述要使用的写入redis的命令
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SADD);
        }

        // 使用数据的那部分作为key 所有传感器是一组
        @Override
        public String getKeyFromData(WaterSensor data) {
            return "set";
        }

        // 使用数据的那部分作为value
        @Override
        public String getValueFromData(WaterSensor data) {
            return JSON.toJSONString(data);
        }
    }
}
