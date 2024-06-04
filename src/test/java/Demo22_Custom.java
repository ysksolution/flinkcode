

import com.sjfood.sjfood.gmallrealtime.bean.WaterSensor;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Created by Smexy on 2022/10/24
 *
 *      自定义Sink，向Mysql写数据
 */
public class Demo22_Custom
{
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
           .addSink(new MySink());

        try {
                    env.execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }

    }

    /*
            只有RickXXXFunction才有
            向Mysql写，一个Task至少需要创建一个Connection，才能连上Mysql
                    open()

            Task运行结束，释放连接
                    close()

             MySink extends RichSinkFunction<WaterSensor>
                    间接 MySink  extends AbstractRichFunction  implements SinkFunction<IN>

            ------------------
                主键如何设置:
                        如果是log:  没主键，后果可能重复
                        参考粒度设置主键:
                                希望一种传感器是一行，id作为主键
                                如果一种传感器在某个时间的水位是一行，id,ts作为联合主键
     */

    /*
    CREATE TABLE `water` (
  `id` varchar(100) NOT NULL,
  `ts` bigint(20) NOT NULL,
  `vc` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`,`ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
     */
    private static class MySink extends RichSinkFunction<WaterSensor>
    {

        private Connection connection;

        //Task创建时，调用一次
        @Override
        public void open(Configuration parameters) throws Exception {


            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(
                "jdbc:mysql://hadoop102:3306/student?useSSL=false&useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true",
                    Constant.MYSQL_USER,
                Constant.MYSQL_PASSWD
            );
        }

        //Task关闭时调用一次
        @Override
        public void close() throws Exception {
            if (connection != null){
                connection.close();
            }
        }

        //一条数据调用一次
        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            //具有幂等性
            String sql = "replace into water values(?,?,?) ";

            PreparedStatement ps = connection.prepareStatement(sql);

            //填充语句
            ps.setString(1,value.getId());
            ps.setLong(2,value.getTs());
            ps.setInt(3,value.getVc());

            //执行
            ps.execute();
        }
    }
}
