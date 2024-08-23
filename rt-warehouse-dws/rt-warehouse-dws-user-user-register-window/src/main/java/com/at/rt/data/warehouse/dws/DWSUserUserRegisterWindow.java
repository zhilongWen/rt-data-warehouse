package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import com.at.rt.data.warehouse.bean.UserRegisterBean;
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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

/**
 * Hello world!
 *
 * @author wenzhilong
 */
public class DWSUserUserRegisterWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);

        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        SingleOutputStreamOperator<String> resultStream = env
                .fromSource(
                        FlinkKafkaUtil.getConsumer(parameterTool, "dwd_user_register"),
                        WatermarkStrategy.noWatermarks(),
                        "dwd_user_register"
                )
                //  1.对流中数据类型进行转
                .map(JSONWrapper::parseJSONObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JsonNode>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                                .withIdleness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<JsonNode>) (element, recordTimestamp)
                                                -> DateUtil.dateTimeToTs(JSONWrapper.getString(element, "create_time"))
                                )
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .aggregate(
                        new AggregateFunction<JsonNode, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(JsonNode value, Long acc) {
                                return acc + 1;
                            }

                            @Override
                            public Long getResult(Long acc) {
                                return acc;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return a + b;
                            }
                        },
                        new ProcessAllWindowFunction<Long, UserRegisterBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<Long, UserRegisterBean, TimeWindow>.Context ctx,
                                                Iterable<Long> elements, Collector<UserRegisterBean> out) throws Exception {
                                Long result = elements.iterator().next();

                                out.collect(
                                        UserRegisterBean.builder()
                                                .stt(DateUtil.tsToDateTime(ctx.window().getStart()))
                                                .edt(DateUtil.tsToDateTime(ctx.window().getEnd()))
                                                .curDate(DateUtil.tsToDateForPartition(ctx.window().getEnd()))
                                                .registerCt(result)
                                                .build()
                                );
                            }
                        }
                )
                .map(JSONWrapper::toJSONStringSnake)
                .setParallelism(1);

        Properties props = new Properties();
        props.setProperty("format", "json");
        // 每行一条 json 数据
        props.setProperty("read_json_by_line", "true");

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                // 设置 doris 的连接参数
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes("10.211.55.102:7030")
                        .setTableIdentifier("gmall_realtime.dws_user_user_register_window")
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
                        .setBufferCount(2)
                        //用于缓存stream load数据的缓冲区大小: 默认 1M
                        .setBufferSize(1024 * 1024)
                        .setMaxRetries(3)
                        // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .setStreamLoadProp(props)
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();

        resultStream.sinkTo(sink).setParallelism(1);

        env.execute();
    }
}
