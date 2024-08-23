package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import com.at.rt.data.warehouse.bean.TrafficHomeDetailPageViewBean;
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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 首页、详情页独立访客聚合统计
 *
 * @author wenzhilong
 */
public class DWSTrafficHomeDetailPageViewWindow {

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
                // 2.过滤首页以及详情页
                .filter(
                        (FilterFunction<JsonNode>) value -> {
                            String pageId = JSONWrapper.getString(value, Arrays.asList("page", "page_id"));
                            return "home".equals(pageId) || "good_detail".equals(pageId);
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
                // 4.按照mid进行分组
                .keyBy(obj -> JSONWrapper.getString(obj, obj.toString().substring(0, 5), Arrays.asList("common", "mid")))
                // 5.使用flink的状态编程  判断是否为首页以及详情页的独立访客   并将结果封装为统计的实体类对象
                .process(new ParsePageLogFunc())
                // 6.开窗
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 7.聚合
                .reduce(
                        new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1,
                                                                        TrafficHomeDetailPageViewBean value2) throws Exception {
                                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                                return value1;
                            }
                        },
                        new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window,
                                              Iterable<TrafficHomeDetailPageViewBean> values,
                                              Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                                TrafficHomeDetailPageViewBean viewBean = values.iterator().next();
                                String stt = DateUtil.tsToDateTime(window.getStart());
                                String edt = DateUtil.tsToDateTime(window.getEnd());
                                String curDate = DateUtil.tsToDate(window.getStart());
                                viewBean.setStt(stt);
                                viewBean.setEdt(edt);
                                viewBean.setCurDate(curDate);
                                out.collect(viewBean);
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
                        .setTableIdentifier("gmall_realtime.dws_traffic_home_detail_page_view_window")
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
