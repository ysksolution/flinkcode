
import com.sjfood.sjfood.gmallrealtime.app.BaseSQLAPP;
import com.sjfood.sjfood.gmallrealtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/8/19:16
 * @Package_name: PACKAGE_NAME
 */
public class LeftJoin extends BaseSQLAPP {

    public static void main(String[] args) {
        new LeftJoin().init(
                2003,
                    2,
                "BaseSQLAPP"
        );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        //join的时候，这种数据在状态中保存的时间
//        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(20));
        tEnv.executeSql("create table t1 (" +
                " id int, "+
                " name string "+
                ")"+ SQLUtil.getKafkaSourceDDL("t1","t1","csv")
        );

        tEnv.executeSql("create table t2 (" +
                " id int, "+
                " age int "+
                ")"+ SQLUtil.getKafkaSourceDDL("t2","t2","csv")
        );

        Table table = tEnv.sqlQuery(" select " +
                "t1.id," +
                "t1.name," +
                "t2.age" +
                " from t1 " +
                " left join t2 " +
                " on t1.id = t2.id "
        );

//        tEnv.createTemporaryView("result",table);


//        table.execute().print();

        tEnv.executeSql("create table t3(" +
                "id int," +
                "name string," +
                "age int," +
                "primary key (id) not enforced"+
                ")"
                +SQLUtil.getUpsertKafkaDDL("t3","json"));


        table.executeInsert("t3");

    }
}
