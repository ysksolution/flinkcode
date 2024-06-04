package com.sjfood.sjfood.gmallrealtime.util;


import com.sjfood.sjfood.gmallrealtime.common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/4/11:50
 * @Package_name: com.atguigu.gmall.realtime.util
 */
public class FileSourceUtil {

    public static SourceFunction<String> getKafkaSource(String groupId, String topic) {

        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        pros.setProperty("group.id",groupId);
        //消费数据的时候，应该只消费已提交的数据
        pros.setProperty("isolation.level","read_committed");
        FlinkKafkaConsumer<String> source = new FlinkKafkaConsumer<>(
                topic,
                new KafkaDeserializationSchema<String>() {

                    //返回流中数据的类型信息
//                    TypeInformation
                    @Override
                    public TypeInformation<String> getProducedType() {
                        //内置的类型
                        return Types.STRING();
                        //类型不包括泛型
//                        return TypeInformation.of(String.class);
                        //泛型套泛型List<String>，一般不用
//                        return TypeInformation.of(new TypeHint<String>() {});
                    }
                    //要不要停止流，流分为有界和无界，true是有界流，kafka读取数据，应该永远不停
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    //真正的反序列化
                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                        if(record.value() != null){
                            return new String(record.value(), StandardCharsets.UTF_8);
                        }
                        return null;
                    }
                },
                pros
        );
        //从最新的offset消费：如果没有消费过，则从最新的开始消费，如果ck中有消费记录，则从上次的位置继续消费
//        source.setStartFromLatest();
        source.setStartFromEarliest();
        return source;
    }
}

