package com.sjfood.sjfood.gmallrealtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.AtguiguiUtil;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/10/8:42
 * @Package_name: com.atguigu.gmallrealtime.app.dwd.log
 */
@Slf4j
public class Dwd_01_DwdBaseLogApp extends BaseAPPV1 {


    private final String START = "start";
    private final String PAGE = "page";
    private final String ACTION = "action";
    private final String ERR = "err";
    private final String DISPLAY = "display";


    public static void main(String[] args) {
        new Dwd_01_DwdBaseLogApp().init(
                3001,2,"Dwd_01_DwdBaseLogApp", Constant.TOPIC_ODS_LOG
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
       // 1.读取 ods_log 数据
//        stream.print();//验证是否可以采集到数据

//        2.对数据做 etl
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);

        //3.纠正新老客户
        SingleOutputStreamOperator<JSONObject> validateStream = validateNewOrOld(etledStream);
        //打印流数据的时候容易和java混淆，不可以使用sout和打印日志
        //需要使用print()方法
//        validateStream.print();

        //4.分流
        HashMap<String, DataStream<JSONObject>> streams = splitStream(validateStream);

        //5.不同的流的数据写入到不同的topic中
        writeToKafka(streams);
    }

    private void writeToKafka(HashMap<String, DataStream<JSONObject>> streams) {

        streams.get(PAGE).map(JSONAware::toJSONString).addSink(FlinkSinKUtil.getKafkaSink(Constant.DWD_TRAFFIC_PAGE));
        streams.get(ERR).map(JSONAware::toJSONString).addSink(FlinkSinKUtil.getKafkaSink(Constant.DWD_TRAFFIC_ERR));
        streams.get(DISPLAY).map(JSONAware::toJSONString).addSink(FlinkSinKUtil.getKafkaSink(Constant.DWD_TRAFFIC_DISPLAY));
        streams.get(ACTION).map(JSONAware::toJSONString).addSink(FlinkSinKUtil.getKafkaSink(Constant.DWD_TRAFFIC_ACTION));
        streams.get(START).map(JSONAware::toJSONString).addSink(FlinkSinKUtil.getKafkaSink(Constant.DWD_TRAFFIC_START));

    }

    private HashMap<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> validateStream) {
        //定义侧输出流
            //必须要加{},加了才代表的是一个匿名类，在运行的时候才会指定数据类型是JSONObject类型，不加会报错
            OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display"){};
            OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("action"){};
            OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("action"){};
            OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page"){};


        /*
            侧输出流：
                主流：启动日志
                侧输出流：活动，曝光，错误，页面

         */
        SingleOutputStreamOperator<JSONObject> startStream = validateStream.process(new ProcessFunction<JSONObject, JSONObject>() {


            @Override
            public void processElement(JSONObject obj, Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject start = obj.getJSONObject("start");


                if (start != null) {
                    out.collect(obj);
                } else {
                    //启动日志和其他日志是互斥的
                    //但是活动，曝光，错误，页面这四种日志不会互斥，一条数据中可能会存在
                    //这四种日志中的多种日志。即同时存在多种日志。
                    //曝光日志
                    JSONObject common = obj.getJSONObject("common");
                    JSONObject page = obj.getJSONObject("page");
                    JSONArray displays = obj.getJSONArray("displays");

                    Long ts = obj.getLong("ts");
                    if (displays != null) {

                        //每条曝光数据曝光了多个商品，应该把曝光拍平，每个商品对应每个曝光
                        for (int i = 0; i < displays.size(); i++) {

                            JSONObject display = displays.getJSONObject(i);
                            display.putAll(common);
                            display.put("ts", ts);

                            if (page != null) {
                                display.putAll(page);
                            }

                            ctx.output(displayTag, display);

                        }
                        //把曝光数据移除
                        obj.remove("displays");
                    }

                    //action
                    JSONArray actions = obj.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.putAll(common);
                            //action本身有ts，ts不用放进去

                            if (page != null) {
                                action.putAll(page);
                            }
                            ctx.output(actionTag, action);
                        }
                        //把动作数据移除
                        obj.remove("actions");
                    }


                    //error
                    JSONObject error = obj.getJSONObject("error");
                    if (error != null) {

                        ctx.output(errTag, obj);

                    }
                    obj.remove("error");

                    //page 基本上每一个页面中都有page
                    if (page != null) {
                        ctx.output(pageTag, obj);
                    }


                }
            }
        });

        DataStream<JSONObject> pageStream = startStream.getSideOutput(pageTag);
        DataStream<JSONObject> errStream = startStream.getSideOutput(errTag);
        DataStream<JSONObject> displayStream = startStream.getSideOutput(displayTag);
        DataStream<JSONObject> actionStream = startStream.getSideOutput(actionTag);

        //想办法返回5个流：
        //集合，数组，元组，Tuple5 它们都有顺序，必须要记住顺序，不好用，应该可以实现随机读取
        //使用map集合
        HashMap<String, DataStream<JSONObject>> streams = new HashMap<>();
        streams.put(START,startStream);
        streams.put(PAGE,pageStream);
        streams.put(ACTION,actionStream);
        streams.put(ERR,errStream);
        streams.put(DISPLAY,displayStream);

        return streams;



    }

    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> stream){
     /*
      如何纠正新老用户：is_new
        错误：把老用户识别成了新用户
            当是新用户的时候才有必要纠正
      例子：比如一个用户在6号的时候访问过，但是在7号的时候发现该用户是新用户
      保存一个状态：存储的是当前mid的首日访问的日期：年月日
      is_new = 1:
        && state != null
          判断这条数据的日期是否与状态一致，如果一致，不用纠正
          如果日期不一致，比如状态是6号，这条数据的日期是7号，那么就需要进行纠正
       state == null
            首次访问
                不需要纠正
                把日期存入到状态中

    */
    return stream
             .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
             .map(new RichMapFunction<JSONObject, JSONObject>() {
                 private ValueState<String> firstVisitDateState;

                 @Override
                 public void open(Configuration parameters) throws Exception {
                     //每个key都有状态
                     //0-5的窗口的状态，能否在5-10的窗口使用。
                     //键控，键控，状态只是键有关，和窗口无关，不论在哪个窗口都可以使用，
                     //前提是不进行clear清理，如果清理了，那么就不能进行访问了
                     firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDateState",String.class));
//                     System.out.println("firstVisitDateState:" + firstVisitDateState);
                 }

                 @Override
                 public JSONObject map(JSONObject obj) throws Exception {

                     //获取当前的mid的首次访问日期
                     String firstVisitDate = firstVisitDateState.value();
                     //获取当前数据的日期
                     Long ts = obj.getLong("ts");
                     String today = AtguiguiUtil.tsToDate(ts);

                     //处理is_new=1的数据
                     JSONObject common = obj.getJSONObject("common");
                     String isNew = common.getString("is_new");
                     if ("1".equals(isNew)) {
                         if ( firstVisitDate == null) {
                             //首次访问
                             //更新状态
                             firstVisitDateState.update(today);
//                             System.out.println("值是："+firstVisitDateState.value());
                         }else if (!today.equals(firstVisitDate)){
                             //is_new有误：应该是0
                             common.put("is_new","0");
                         }
                         //假设有一个用户6号访问的时候是一个1，7号访问的是0，8号的时候是1
                         //程序在7好的时候重启了，清空了7号的数据，7号的数据是0，所以在8号的时候
                         //获取的状态是null，按照上述的程序，状态是null，得到的是新用户，这就有bug了
                         //解决办法：知道7号是老用户，但是状态是null的情况，我们选择在7号的时候，
                         //在状态中添加6号的日期，因为我们知道7号一定不是新用户，所以给一个6号的日期

                     }else {
                         //老用户：如果状态是null，应该把状态设置一个比较早的日期：比如昨天
                         if(firstVisitDate == null){
                             String  yesterday = AtguiguiUtil.tsToDate(ts - (24 * 60 * 60 * 1000));
                             firstVisitDateState.update(yesterday);
                         }

                     }
                     return obj;
                 }
             });





    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        //只需要保持json格式即可，但是工作中这块的清洗比较复杂
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                try {
                    JSON.parseObject(value);
                } catch (Exception e) {
                  log.warn("日志格式不对，不是合法的json日志："+value);
                    return false;
                }
              return true;
            }
        })
           .map(JSONObject::parseObject);


    }
}
