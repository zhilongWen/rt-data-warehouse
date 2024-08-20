package com.at.rt.data.warehouse;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author wenzhilong
 */
public class DWDInteractionCommentInfo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("\n"
                + "CREATE TABLE topic_db (\n"
                + "    `database` string,\n"
                + "    `table` string,\n"
                + "    `type` string,\n"
                + "    `ts` bigint,\n"
                + "    `data` MAP<string, string>,\n"
                + "    `old` MAP<string, string>,\n"
                + "    pt as proctime(),\n"
                + "    et as to_timestamp_ltz(ts, 0), \n"
                + "    watermark for et as et - interval '3' second \n"
                + ")\n"
                + "WITH (\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'ODS_BASE_DB',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = '\" + groupId + \"',\n"
                + "    'scan.startup.mode' = 'latest-offset',\n"
                + "    'format' = 'json'\n"
                + ")");


        System.out.println("Hello World!");


    }
}
