package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import com.at.rt.data.warehouse.bean.CartAddUuBean;
import com.at.rt.data.warehouse.utils.DateUtil;
import com.at.rt.data.warehouse.utils.FlinkKafkaUtil;
import com.at.rt.data.warehouse.utils.JSONWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
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
 * 交易域加购各窗口汇总表
 *
 * @author wenzhilong
 */
public class DWSTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);

        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        // {"id":"32374","user_id":"84","sku_id":"24","sku_num":"2","ts":1724342031}
        env
                .fromSource(
                        FlinkKafkaUtil.getConsumer(parameterTool, "dwd_trade_cart_add"),
                        WatermarkStrategy.noWatermarks(),
                        "dwd_trade_cart_add"
                )
                //  1.对流中数据类型进行转
                .map(JSONWrapper::parseJSONObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JsonNode>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withIdleness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<JsonNode>) (element, recordTimestamp)
                                                -> JSONWrapper.getLong(element, "ts") * 1000L
                                )
                )
                // 4.按照mid进行分组
                .keyBy(obj -> JSONWrapper.getString(obj, obj.toString().substring(0, 5), Collections.singletonList("user_id")))
                .process(new KeyedProcessFunction<String, JsonNode, CartAddUuBean>() {

                    private ValueState<String> lastCartAddDateState;

                    @Override
                    public void open(Configuration parameters) {
                        lastCartAddDateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("lastCartAddDate", String.class));
                    }

                    @Override
                    public void processElement(JsonNode value,
                                               KeyedProcessFunction<String, JsonNode, CartAddUuBean>.Context ctx,
                                               Collector<CartAddUuBean> out) throws Exception {

                        String lastCartAddDate = lastCartAddDateState.value();
                        long ts = value.get("ts").asLong() * 1000;
                        String today = DateUtil.tsToDate(ts);

                        if (!today.equals(lastCartAddDate)) {
                            lastCartAddDateState.update(today);

                            out.collect(new CartAddUuBean("", "", "", 1L));
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<CartAddUuBean>() {
                            @Override
                            public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>.Context ctx,
                                                Iterable<CartAddUuBean> elements,
                                                Collector<CartAddUuBean> out) throws Exception {
                                CartAddUuBean bean = elements.iterator().next();
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
