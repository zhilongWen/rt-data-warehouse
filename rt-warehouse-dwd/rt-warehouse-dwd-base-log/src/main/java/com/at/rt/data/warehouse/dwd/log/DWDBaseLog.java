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


        env.execute();
    }
}
