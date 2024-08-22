package com.at.rt.data.warehouse.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.StreamExecEnvConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author wenzhilong
 */
public class MainTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);

        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        String sql = parameterTool.get("sql");

        System.out.println(sql);

        env
                .fromSource(
                        KafkaSource.<String>builder()
                                .setBootstrapServers("hadoop102:9092")
                                .setTopics("dwd_traffic_page")
                                .setGroupId("dwd_traffic_page-test")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        ""
                )
                .setParallelism(1)
                .flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                        try {

                            JSONObject jsonObject = JSON.parseObject(value);

                            JSONObject common = jsonObject.getJSONObject("common");
                            String uid = common.getString("uid");
                            if (StringUtils.isBlank(uid)) {
                                return;
                            }

                            out.collect(jsonObject);

                        } catch (Exception e) {
                            System.out.println("error : " + e);
                        }

                    }
                })
                .setParallelism(1)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<JSONObject>() {
                                            @Override
                                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                                return element.getLong("ts");
                                            }
                                        }
                                )
                )
                .setParallelism(1)
                .keyBy(r -> r.getJSONObject("common").getString("uid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<JSONObject, String, String, TimeWindow>.Context context, Iterable<JSONObject> elements, Collector<String> out) throws Exception {

                        long size = elements.spliterator().getExactSizeIfKnown();

                        out.collect("key = " + key + " win [ " + new Timestamp(context.window().getStart()) + " - " + new Timestamp(context.window().getEnd()) + " ) 中有 " + size + " 条记录");

                    }
                })
                .setParallelism(1)
                .print()
                .setParallelism(1);


        env.execute();
    }
}
