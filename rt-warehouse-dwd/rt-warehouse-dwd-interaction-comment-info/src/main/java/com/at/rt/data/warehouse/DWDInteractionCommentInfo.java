package com.at.rt.data.warehouse;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author wenzhilong
 */
public class DWDInteractionCommentInfo {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE topic_db (\n"
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
                + "    'properties.group.id' = 'DWDInteractionCommentInfo',  \n"
                + "    'scan.startup.mode' = 'earliest-offset',\n"
                + "    'format' = 'json'\n"
                + ")");

        // 过滤出评论数据
        Table commonInfoTable = tableEnv.sqlQuery("select\n"
                + "    `data`['id'] id,\n"
                + "    `data`['user_id'] user_id,\n"
                + "    `data`['sku_id'] sku_id,\n"
                + "    `data`['appraise'] appraise,\n"
                + "    `data`['comment_txt'] comment_txt,\n"
                + "    ts,\n"
                + "    pt\n"
                + "from topic_db where `table`='comment_info' and `type`='insert'");
        tableEnv.createTemporaryView("comment_info", commonInfoTable);

        // 从HBase中读取字典数据 创建动态表
        tableEnv.executeSql("CREATE TABLE base_dic (\n"
                + "    dic_code string,\n"
                + "    info ROW<dic_name string>,\n"
                + "    PRIMARY KEY (dic_code) NOT ENFORCED\n"
                + ")\n"
                + " WITH (\n"
                + "    'connector' = 'hbase-2.2',\n"
                + "    'table-name' = 'gmall:dim_base_dic',\n"
                + "    'zookeeper.quorum' = 'hadoop102:2181',\n"
                + "    'lookup.async' = 'true',\n"
                + "    'lookup.cache' = 'PARTIAL',\n"
                + "    'lookup.partial-cache.max-rows' = '500',\n"
                + "    'lookup.partial-cache.expire-after-write' = '1 hour',\n"
                + "    'lookup.partial-cache.expire-after-access' = '1 hour'\n"
                + ")");

        // 将评论表和字典表进行关联
        Table joinTable = tableEnv.sqlQuery("SELECT\n"
                + "    id,\n"
                + "    user_id,\n"
                + "    sku_id,\n"
                + "    appraise,\n"
                + "    dic.dic_name appraise_name,\n"
                + "    comment_txt,\n"
                + "    ts\n"
                + "FROM comment_info AS c\n"
                + "  JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS dic\n"
                + "    ON c.appraise = dic.dic_code");

        // 将关联的结果写到kafka主题中
        tableEnv.executeSql("CREATE TABLE dwd_interaction_comment_info (\n"
                + "    id string,\n"
                + "    user_id string,\n"
                + "    sku_id string,\n"
                + "    appraise string,\n"
                + "    appraise_name string,\n"
                + "    comment_txt string,\n"
                + "    ts bigint,\n"
                + "    PRIMARY KEY (id) NOT ENFORCED\n"
                + ") \n"
                + "WITH (\n"
                + "    'connector' = 'upsert-kafka',\n"
                + "    'topic' = 'dwd_interaction_comment_info',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'key.format' = 'json',\n"
                + "    'value.format' = 'json'\n"
                + ")");

        joinTable.executeInsert("dwd_interaction_comment_info");
    }
}
