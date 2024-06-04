import com.sjfood.sjfood.gmallrealtime.common.Constant;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;

public class UpsertKafkaExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceTableDDL = "CREATE TABLE source_table(" +
                "user_id int," +
                "salary bigint, " +
                "proc as PROCTIME()," +
                "PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = 'source2'," +
                "  'properties.bootstrap.servers' = '"+Constant.KAFKA_BROKERS+"'," +
                "  'properties.group.id' = 'user_log', " +
                "  'key.format' = 'csv'," +
                "  'value.format' = 'csv'" +
                ")";

        tableEnv.executeSql(sourceTableDDL);

        System.out.println("表数据如下：");
        tableEnv.sqlQuery("select * from source_table").execute().print();

        env.execute();


    }
}
