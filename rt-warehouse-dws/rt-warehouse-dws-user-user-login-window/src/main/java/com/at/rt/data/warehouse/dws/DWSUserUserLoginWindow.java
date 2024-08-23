package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import com.at.rt.data.warehouse.bean.UserLoginBean;
import com.at.rt.data.warehouse.utils.DateUtil;
import com.at.rt.data.warehouse.utils.FlinkKafkaUtil;
import com.at.rt.data.warehouse.utils.JSONWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 独立用户以及回流用户聚合统计
 *
 * @author wenzhilong
 */
public class DWSUserUserLoginWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);

        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        SingleOutputStreamOperator<String> resultStream = env
                .fromSource(
                        FlinkKafkaUtil.getConsumer(parameterTool, "dwd_traffic_page"),
                        WatermarkStrategy.noWatermarks(),
                        "dwd_traffic_page"
                )
                //  1.对流中数据类型进行转
                .map(JSONWrapper::parseJSONObject)
                // 2.过滤出登录行为
                .filter(
                        (FilterFunction<JsonNode>) value -> {

                            String uid = JSONWrapper.getString(value, Arrays.asList("common", "uid"));
                            String lastPageId = JSONWrapper.getString(value, Arrays.asList("page", "last_page_id"));

                            return StringUtils.isNotEmpty(uid)
                                    && ("login".equals(lastPageId)
                                    || StringUtils.isEmpty(lastPageId));
                        }
                )
                // 3.指定Watermark的生成策略以及提取事件时间字段
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JsonNode>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withIdleness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<JsonNode>) (element, recordTimestamp)
                                                -> JSONWrapper.getLong(element, "ts")
                                )
                )
                // 4.按照 uid 进行分组
                .keyBy(obj -> JSONWrapper.getString(obj, obj.toString().substring(0, 5), Arrays.asList("common", "uid")))
                // 5.使用Flink的状态编程  判断是否为独立用户以及回流用户
                .process(new KeyedProcessFunction<String, JsonNode, UserLoginBean>() {

                    private transient ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastLoginDateState", String.class);
                        lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JsonNode value,
                                               KeyedProcessFunction<String, JsonNode, UserLoginBean>.Context ctx,
                                               Collector<UserLoginBean> out) throws Exception {

                        //获取上次登录日期
                        String lastLoginDate = lastLoginDateState.value();
                        //获取当前登录日期
                        Long ts = value.get("ts").asLong();
                        String curLoginDate = DateUtil.tsToDate(ts);

                        long uuCt = 0L;
                        long backCt = 0L;
                        if (StringUtils.isNotEmpty(lastLoginDate)) {
                            //若状态中的末次登录日期不为 null，进一步判断。
                            if (!lastLoginDate.equals(curLoginDate)) {
                                //如果末次登录日期不等于当天日期则独立用户数 uuCt 记为 1，并将状态中的末次登录日期更新为当日，进一步判断。
                                uuCt = 1L;
                                lastLoginDateState.update(curLoginDate);
                                //如果当天日期与末次登录日期之差大于等于8天则回流用户数backCt置为1。
                                long day = (ts - DateUtil.dateToTs(lastLoginDate)) / 1000 / 60 / 60 / 24;
                                if (day >= 8) {
                                    backCt = 1L;
                                }
                            }
                        } else {
                            //如果状态中的末次登录日期为 null，将 uuCt 置为 1，backCt 置为 0，并将状态中的末次登录日期更新为当日。
                            uuCt = 1L;
                            lastLoginDateState.update(curLoginDate);
                        }

                        if (uuCt != 0L || backCt != 0L) {
                            out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<UserLoginBean>() {
                            @Override
                            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) {
                                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                                return value1;
                            }
                        },
                        new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) {
                                UserLoginBean bean = values.iterator().next();
                                String stt = DateUtil.tsToDateTime(window.getStart());
                                String edt = DateUtil.tsToDateTime(window.getEnd());
                                String curDate = DateUtil.tsToDate(window.getStart());
                                bean.setStt(stt);
                                bean.setEdt(edt);
                                bean.setCurDate(curDate);
                                out.collect(bean);
                            }
                        }
                )
                .map(JSONWrapper::toJSONStringSnake);

        // 8.将聚合的结果写到Doris

        Properties props = new Properties();
        props.setProperty("format", "json");
        // 每行一条 json 数据
        props.setProperty("read_json_by_line", "true");

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                // 设置 doris 的连接参数
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes("10.211.55.102:7030")
                        .setTableIdentifier("gmall_realtime.dws_user_user_login_window")
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

        resultStream.sinkTo(sink);

        env.execute();
    }
}
