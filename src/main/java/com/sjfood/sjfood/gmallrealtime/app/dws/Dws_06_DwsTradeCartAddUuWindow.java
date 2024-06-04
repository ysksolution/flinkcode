package com.sjfood.sjfood.gmallrealtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sjfood.sjfood.gmallrealtime.app.BaseAPPV1;
import com.sjfood.sjfood.gmallrealtime.bean.CartAddUuBean;
import com.sjfood.sjfood.gmallrealtime.common.Constant;
import com.sjfood.sjfood.gmallrealtime.util.AtguiguiUtil;
import com.sjfood.sjfood.gmallrealtime.util.FlinkSinKUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/15/8:27
 * @Package_name: com.sjfood.sjfood.gmallrealtime.app.dws
 */
public class Dws_06_DwsTradeCartAddUuWindow extends BaseAPPV1 {

    public static void main(String[] args) {

        new Dws_06_DwsTradeCartAddUuWindow().init(
                4006,2,"Dws_06_DwsTradeCartAddUuWindow", Constant.DWD_TRADE_CART_ADD
        );
    }



    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
    /*
        统计加购用户数，获取用户最后加购日期

        从 Kafka 读取用户加购明细数据，统计各窗口加购独立用户数，写入 ClickHouse
        1）从 Kafka 加购明细主题读取数据
        2）转换数据结构
            将流中数据由 String 转换为 JSONObject。
        3）设置水位线
        4）按照用户 id 分组
        5）过滤独立用户加购记录
            运用 Flink 状态编程，将用户末次加购日期维护到状态中。
            如果末次登录日期为 null 或者不等于当天日期，则保留数据并更新状态，否则丢弃，不做操作。
        6）开窗、聚合
            统计窗口中数据条数即为加购独立用户数，补充窗口起始时间、关闭时间，将时间戳字段置为当前系统时间，发送到下游。
        7）将数据写入 ClickHouse。


     */

            stream
                    .map(JSON::parseObject)
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy
                                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                    .withTimestampAssigner((obj,ts) -> obj.getLong("ts") * 1000)
                                    .withIdleness(Duration.ofSeconds(20))
                    )
                    .keyBy(obj -> obj.getString("user_id"))
                    .flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

                        private ValueState<String> lastCartAddDateState;

                        @Override
                        public void open(Configuration parameters) throws Exception {

                            lastCartAddDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastCartAddDateState", String.class));

                        }

                        @Override
                        public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {

                            Long ts = value.getLong("ts") * 1000; //ts是maxwell生成的，单位是秒
                            Long cartAddUuCt = 0L;

                            String today = AtguiguiUtil.tsToDate(ts);

                            String lastCartAddDate = lastCartAddDateState.value();

                            if (!today.equals(lastCartAddDate)) {
                                cartAddUuCt = 1L;
                                lastCartAddDateState.update(today);

                            }

                            if (cartAddUuCt == 1) {
                                out.collect(new CartAddUuBean("","",cartAddUuCt,ts));
                            }
                        }
                    })
                    .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                    .reduce(new ReduceFunction<CartAddUuBean>() {
                                @Override
                                public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {

                                    value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                                    return value1;
                                }
                            },
                            new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {

                                @Override
                                public void process(Context context, Iterable<CartAddUuBean> elements, Collector<CartAddUuBean> out) throws Exception {

                                    CartAddUuBean bean = elements.iterator().next();

                                    bean.setStt(AtguiguiUtil.tsToDateTime(context.window().getStart()));
                                    bean.setEdt(AtguiguiUtil.tsToDateTime(context.window().getEnd()));

                                    bean.setTs(System.currentTimeMillis());

                                    out.collect(bean);
                                }
                            }


                    )
                  .addSink(FlinkSinKUtil.getClickHouseSink("dws_trade_cart_add_uu_window ",CartAddUuBean.class));


    }
}
