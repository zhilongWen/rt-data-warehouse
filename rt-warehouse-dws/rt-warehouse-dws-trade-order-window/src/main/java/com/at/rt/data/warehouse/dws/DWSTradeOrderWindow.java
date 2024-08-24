package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import com.at.rt.data.warehouse.bean.TradeOrderBean;
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
 * 交易域下单各窗口汇总表
 *
 * @author wenzhilong
 */
public class DWSTradeOrderWindow {

    public static void main(String[] args)
            throws Exception {

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);

        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        // {"id":"13518","order_id":"5079","user_id":"351","sku_id":"33","sku_name":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 粉邂逅淡香水35ml","province_id":"20","activity_id":null,"activity_rule_id":null,"coupon_id":null,"date_id":"2024-08-22","create_time":"2024-08-22 23:53:51","sku_num":"3","split_original_amount":"1464.0000","split_activity_amount":null,"split_coupon_amount":null,"split_total_amount":"1464.0","ts":1724342031}
        env
                .fromSource(
                        FlinkKafkaUtil.getConsumer(parameterTool, "dwd_trade_order_detail"),
                        WatermarkStrategy.noWatermarks(),
                        "dwd_trade_order_detail"
                )
                //  1.对流中数据类型进行转
                .map(JSONWrapper::parseJSONObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JsonNode>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.get("ts").asLong() * 1000L)
                                .withIdleness(Duration.ofSeconds(2))
                )
                .keyBy(obj -> JSONWrapper.getString(obj, obj.toString().substring(0, 5), Collections.singletonList("user_id")))
                .process(new KeyedProcessFunction<String, JsonNode, TradeOrderBean>() {

                    private ValueState<String> lastOrderDateState;

                    @Override
                    public void open(Configuration parameters) {
                        lastOrderDateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("lastOrderDate", String.class));
                    }

                    @Override
                    public void processElement(JsonNode value,
                                               KeyedProcessFunction<String, JsonNode, TradeOrderBean>.Context ctx,
                                               Collector<TradeOrderBean> out) throws Exception {

                        long ts = value.get("ts").asLong() * 1000;

                        String today = DateUtil.tsToDate(ts);
                        String lastOrderDate = lastOrderDateState.value();

                        long orderUu = 0L;
                        long orderNew = 0L;
                        if (!today.equals(lastOrderDate)) {
                            orderUu = 1L;
                            lastOrderDateState.update(today);

                            if (lastOrderDate == null) {
                                orderNew = 1L;
                            }

                        }
                        if (orderUu == 1) {
                            out.collect(new TradeOrderBean("", "", "", orderUu, orderNew, ts));
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TradeOrderBean>() {
                            @Override
                            public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>.Context ctx,
                                                Iterable<TradeOrderBean> elements,
                                                Collector<TradeOrderBean> out) throws Exception {
                                TradeOrderBean bean = elements.iterator().next();
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
