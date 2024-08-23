package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import com.at.rt.data.warehouse.bean.TrafficPageViewBean;
import com.at.rt.data.warehouse.utils.FlinkKafkaUtil;
import com.at.rt.data.warehouse.utils.JSONWrapper;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 按照版本、地区、渠道、新老访客对pv、uv、sv、dur进行聚合统计
 *
 * @author wenzhilong
 */
public class DWSTrafficVcChArIsNewPageViewWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);

        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        SingleOutputStreamOperator<String> resStream = env
                .fromSource(
                        FlinkKafkaUtil.getConsumer(parameterTool, "dwd_traffic_page"),
                        WatermarkStrategy.noWatermarks(),
                        "dwd_traffic_page"
                )
                // 1.对流中数据进行类型转换
                .map(JSONWrapper::parseJSONObject)
                // 2.按照 mid 对流中数据进行分组（计算 UV ）
                .keyBy(obj -> JSONWrapper.getString(obj, obj.toString().substring(0, 5), Arrays.asList("common", "key")))
                // 3.再次对流中数据进行类型转换  jsonObj->统计的实体类对象
                .process(new ParseLogKeyFun())
                // 4.指定Watermark以及提取事件时间字段
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withIdleness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<TrafficPageViewBean>) (element, recordTimestamp) -> element.getTs()
                                )
                )
                // 5.分组--按照统计的维度进行分组
                .keyBy((KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>) value -> Tuple4.of(value.getVc(),
                        value.getCh(),
                        value.getAr(),
                        value.getIsNew()))
                // 6.开窗
                // 以滚动事件时间窗口为例，分析如下几个窗口相关的问题
                // 窗口对象时候创建:当属于这个窗口的第一个元素到来的时候创建窗口对象
                // 窗口的起始结束时间（窗口为什么是左闭右开的）
                //      向下取整：long start =TimeWindow.getWindowStartWithOffset(timestamp, (globalOffset + staggerOffset) % size, size);
                // 窗口什么时候触发计算
                //      window.maxTimestamp() <= ctx.getCurrentWatermark()
                // 窗口什么时候关闭
                //      watermark >= window.maxTimestamp() + allowedLateness
                .window(
                        TumblingEventTimeWindows.of(Time.seconds(10))
                )
                // aggregate 累加器和下游传递的不一样
                // .aggregate()
                // reduce 累加器和下游传递的一样
                .reduce(
                        new AggReduceFunc(),
                        new WindowAllAggFunction()
                )
                .map(JSONWrapper::toJSONStringSnake);

        Properties props = new Properties();
        props.setProperty("format", "json");
        // 每行一条 json 数据
        props.setProperty("read_json_by_line", "true");

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                // 设置 doris 的连接参数
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes("10.211.55.102:7030")
                        .setTableIdentifier("gmall_realtime.dws_traffic_vc_ch_ar_is_new_page_view_window")
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

        resStream.sinkTo(sink);

        env.execute();
    }
}
