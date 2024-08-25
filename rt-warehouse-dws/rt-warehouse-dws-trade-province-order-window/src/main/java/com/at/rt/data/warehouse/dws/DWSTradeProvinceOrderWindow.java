package com.at.rt.data.warehouse.dws;

import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.StreamExecEnvConf;
import com.at.rt.data.warehouse.bean.TradeProvinceOrderBean;
import com.at.rt.data.warehouse.function.DimAsyncFunction;
import com.at.rt.data.warehouse.utils.DateUtil;
import com.at.rt.data.warehouse.utils.FlinkKafkaUtil;
import com.at.rt.data.warehouse.utils.JSONWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 省份粒度下单聚合统计
 */
public class DWSTradeProvinceOrderWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);

        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        // {"id":"13518","order_id":"5079","user_id":"351","sku_id":"33","sku_name":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 粉邂逅淡香水35ml","province_id":"20","activity_id":null,"activity_rule_id":null,"coupon_id":null,"date_id":"2024-08-22","create_time":"2024-08-22 23:53:51","sku_num":"3","split_original_amount":"1464.0000","split_activity_amount":null,"split_coupon_amount":null,"split_total_amount":"1464.0","ts":1724342031}
        SingleOutputStreamOperator<TradeProvinceOrderBean> resStream = env
                .fromSource(
                        FlinkKafkaUtil.getConsumer(parameterTool, "dwd_trade_order_detail"),
                        WatermarkStrategy.noWatermarks(),
                        "dwd_trade_order_detail"
                )
                .flatMap(new RichFlatMapFunction<String, TradeProvinceOrderBean>() {
                    @Override
                    public void flatMap(String value, Collector<TradeProvinceOrderBean> out) throws Exception {
                        try {

                            JsonNode jsonNode = JSONWrapper.parseJSONObject(value);

                            BigDecimal orderAmount;
                            JsonNode orderAmountStr = jsonNode.get("split_original_amount");
                            if (orderAmountStr == null) {
                                orderAmount = BigDecimal.ZERO;
                            } else {
                                orderAmount = new BigDecimal(orderAmountStr.asText());
                            }
                            String orderId = JSONWrapper.getString(jsonNode, "order_id");

                            long ts = jsonNode.get("ts").asLong() * 1000L;
                            String id = jsonNode.get("id").asText();

                            out.collect(
                                    TradeProvinceOrderBean.builder()
                                            .provinceId(JSONWrapper.getString(jsonNode, "province_id"))
                                            .orderAmount(orderAmount)
                                            .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                                            .orderDetailId(id)
                                            .ts(ts)
                                            .build()
                            );
                        } catch (Exception e) {
                            System.out.println("parse error " + e);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withIdleness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<TradeProvinceOrderBean>) (element, recordTimestamp) -> element.getTs()
                                )
                )
                // 按照唯一键(订单明细的id)进行分组
                .keyBy(TradeProvinceOrderBean::getOrderDetailId)
                // 去重
                .process(
                        new KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>() {

                            private ValueState<TradeProvinceOrderBean> lastJsonObjState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<TradeProvinceOrderBean> valueStateDescriptor
                                        = new ValueStateDescriptor<>("lastJsonObjState", TradeProvinceOrderBean.class);
                                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                                lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(TradeProvinceOrderBean value, KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>.Context ctx, Collector<TradeProvinceOrderBean> out) throws Exception {

                                if (value.getOrderAmount() == null) {
                                    value.setOrderAmount(BigDecimal.ZERO);
                                }

                                // 从状态中获取上次接收到的数据
                                TradeProvinceOrderBean orderBean = lastJsonObjState.value();

                                if (orderBean != null) {
                                    // 重复  需要对影响到度量值的字段进行取反 发送到下游
                                    orderBean.setOrderAmount(orderBean.getOrderAmount().negate());
                                    out.collect(orderBean);
                                }

                                lastJsonObjState.update(value);
                                out.collect(value);
                            }
                        })
                .keyBy(TradeProvinceOrderBean::getProvinceId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(
                        new ReduceFunction<TradeProvinceOrderBean>() {
                            @Override
                            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                return value1;
                            }
                        },
                        new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                            @Override
                            public void apply(String key, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                                TradeProvinceOrderBean orderBean = input.iterator().next();
                                String stt = DateUtil.tsToDateTime(window.getStart());
                                String edt = DateUtil.tsToDateTime(window.getEnd());
                                String curDate = DateUtil.tsToDate(window.getStart());
                                orderBean.setStt(stt);
                                orderBean.setEdt(edt);
                                orderBean.setCurDate(curDate);
                                orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                                out.collect(orderBean);
                            }
                        }
                );

        Properties props = new Properties();
        props.setProperty("format", "json");
        // 每行一条 json 数据
        props.setProperty("read_json_by_line", "true");

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                // 设置 doris 的连接参数
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes("10.211.55.102:7030")
                        .setTableIdentifier("gmall_realtime.dws_trade_province_order_window")
                        .setUsername("root")
                        .setPassword("root123")
                        .build())
                // 执行参数
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        // stream-load 导入的时候的 label 前缀
                        //.setLabelPrefix("doris-label")
                        // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .disable2PC()
                        .setDeletable(false)
                        // 用于缓存stream load数据的缓冲条数: 默认 3
                        .setBufferCount(3)
                        //用于缓存stream load数据的缓冲区大小: 默认 1M
                        .setBufferSize(1024 * 1024)
                        .setMaxRetries(3)
                        // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .setStreamLoadProp(props)
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();

        AsyncDataStream.unorderedWait(
                        resStream,
                        new DimAsyncFunction<TradeProvinceOrderBean>() {
                            @Override
                            public void addDims(TradeProvinceOrderBean orderBean, JSONObject dimJsonObj) {
                                orderBean.setProvinceName(dimJsonObj.getString("name"));
                            }

                            @Override
                            public String getTableName() {
                                return "dim_base_province";
                            }

                            @Override
                            public String getRowKey(TradeProvinceOrderBean orderBean) {
                                return orderBean.getProvinceId();
                            }
                        },
                        60,
                        TimeUnit.SECONDS
                )
                .map(JSONWrapper::toJSONStringSnake)
                .sinkTo(sink);

        env.execute();
    }
}
