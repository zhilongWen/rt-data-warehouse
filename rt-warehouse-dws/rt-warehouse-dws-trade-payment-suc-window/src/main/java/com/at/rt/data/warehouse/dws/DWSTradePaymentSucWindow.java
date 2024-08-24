package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import com.at.rt.data.warehouse.bean.TradePaymentBean;
import com.at.rt.data.warehouse.utils.DateUtil;
import com.at.rt.data.warehouse.utils.FlinkKafkaUtil;
import com.at.rt.data.warehouse.utils.JSONWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;

/**
 * 交易域支付成功各窗口汇总表
 *
 * @author wenzhilong
 */
public class DWSTradePaymentSucWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);

        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        // {"order_detail_id":"13493","order_id":"5068","user_id":"230","sku_id":"8","sku_name":"Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机","province_id":"2","activity_id":"2","activity_rule_id":"4","coupon_id":null,"payment_type_code":"1102","payment_type_name":"微信","callback_time":"2024-08-22 23:54:11","sku_num":"1","split_original_amount":"8197.0000","split_activity_amount":"235.63","split_coupon_amount":null,"split_payment_amount":"7961.37","ts":1724342032}
        env
                .fromSource(
                        FlinkKafkaUtil.getConsumer(parameterTool, "dwd_trade_order_payment_success"),
                        WatermarkStrategy.noWatermarks(),
                        "dwd_trade_order_payment_success"
                )
                //  1.对流中数据类型进行转
                .map(JSONWrapper::parseJSONObject)
                .keyBy(obj -> JSONWrapper.getString(obj, obj.toString().substring(0, 5), Collections.singletonList("user_id")))
                .process(new KeyedProcessFunction<String, JsonNode, TradePaymentBean>() {

                    private ValueState<String> lastPayDateState;

                    @Override
                    public void open(Configuration parameters) {
                        lastPayDateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("lastPayDate", String.class));
                    }

                    @Override
                    public void processElement(JsonNode value,
                                               KeyedProcessFunction<String, JsonNode, TradePaymentBean>.Context ctx,
                                               Collector<TradePaymentBean> out) throws Exception {

                        String lastPayDate = lastPayDateState.value();
                        long ts = value.get("ts").asLong() * 1000;
                        String today = DateUtil.tsToDate(ts);

                        long payUuCount = 0L;
                        long payNewCount = 0L;
                        if (!today.equals(lastPayDate)) {  // 今天第一次支付成功
                            lastPayDateState.update(today);
                            payUuCount = 1L;

                            if (lastPayDate == null) {
                                // 表示这个用户曾经没有支付过, 是一个新用户支付
                                payNewCount = 1L;
                            }
                        }

                        if (payUuCount == 1) {
                            out.collect(new TradePaymentBean("", "", "", payUuCount, payNewCount, ts));
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradePaymentBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(2))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TradePaymentBean>() {
                            @Override
                            public TradePaymentBean reduce(TradePaymentBean value1, TradePaymentBean value2) throws Exception {
                                value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                                value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>.Context ctx,
                                                Iterable<TradePaymentBean> elements,
                                                Collector<TradePaymentBean> out) throws Exception {
                                TradePaymentBean bean = elements.iterator().next();
                                bean.setStt(DateUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateUtil.tsToDateTime(ctx.window().getEnd()));

                                bean.setCurDate(DateUtil.tsToDateForPartition(ctx.window().getStart()));

                                out.collect(bean);
                            }
                        }
                )
                .map(JSONWrapper::toJSONStringSnake)
                .print();

        env.execute();
    }
}
