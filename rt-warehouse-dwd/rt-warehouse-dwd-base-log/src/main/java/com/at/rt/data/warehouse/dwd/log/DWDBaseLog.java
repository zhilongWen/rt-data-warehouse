package com.at.rt.data.warehouse.dwd.log;

import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.StreamExecEnvConf;
import com.at.rt.data.warehouse.utils.FlinkKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * @author wenzhilong
 */
public class DWDBaseLog {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        OutputTag<String> errorLogTag = new OutputTag<String>("errorLogTag") {
        };

        SingleOutputStreamOperator<JSONObject> etlLogStream = env
                .fromSource(
                        FlinkKafkaUtil.getConsumer(parameterTool, "dwd_base_log_split"),
                        WatermarkStrategy.noWatermarks(),
                        "dwd_base_log_split"
                )
                .process(new DwdBaseLogEtlProcess(errorLogTag));

        etlLogStream.getSideOutput(errorLogTag)
                .sinkTo(FlinkKafkaUtil.getProduct(parameterTool, "error_log_tag"));

        OutputTag<String> errTag = new OutputTag<String>("errTag") {
        };
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };

        // 对新老访客标记进行修复
        SingleOutputStreamOperator<String> pageStream = etlLogStream
                .keyBy(r -> r.getJSONObject("common").getString("mid"))
                .process(new FixedNewAndOldProcessFun())
                .process(new SplitDWDLogStreamFun());

//        SideOutputDataStream<String> errDS = pageStream.getSideOutput(errTag);
//        SideOutputDataStream<String> startDS = pageStream.getSideOutput(startTag);
//        SideOutputDataStream<String> displayDS = pageStream.getSideOutput(displayTag);
//        SideOutputDataStream<String> actionDS = pageStream.getSideOutput(actionTag);
//        pageStream.print("页面:");
//        errDS.print("错误:");
//        startDS.print("启动:");
//        displayDS.print("曝光:");
//        actionDS.print("动作:");

        pageStream.sinkTo(FlinkKafkaUtil.getProduct(parameterTool, "dwd_traffic_page"));
        pageStream.getSideOutput(errTag)
                .sinkTo(FlinkKafkaUtil.getProduct(parameterTool, "dwd_traffic_err"));
        pageStream.getSideOutput(startTag)
                .sinkTo(FlinkKafkaUtil.getProduct(parameterTool, "dwd_traffic_start"));
        pageStream.getSideOutput(displayTag)
                .sinkTo(FlinkKafkaUtil.getProduct(parameterTool, "dwd_traffic_display"));
        pageStream.getSideOutput(actionTag)
                .sinkTo(FlinkKafkaUtil.getProduct(parameterTool, "dwd_traffic_action"));

        env.execute();
    }
}
