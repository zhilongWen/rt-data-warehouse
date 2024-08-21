package com.at.rt.data.warehouse.dwd.cart;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * DWDTradeCartAdd
 *
 * @author wenzhilong
 */
public class DWDTradeCartAdd {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

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
                + "    'properties.group.id' = 'DWDTradeCartAdd',  \n"
                + "    'scan.startup.mode' = 'earliest-offset',\n"
                + "    'format' = 'json'\n"
                + ")");

        Table cartInfoTable = tableEnv.sqlQuery("select \n"
                + "   `data`['id'] id,\n"
                + "   `data`['user_id'] user_id,\n"
                + "   `data`['sku_id'] sku_id,\n"
                + "   if(type='insert',`data`['sku_num'], CAST((CAST(data['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) AS STRING)) sku_num,\n"
                + "   ts\n"
                + "from topic_db \n"
                + "where `table`='cart_info' \n"
                + "and (\n"
                + "    type = 'insert'\n"
                + "    or\n"
                + "    (type='update' and `old`['sku_num'] is not null and (CAST(data['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT)))\n"
                + ")");

        tableEnv.executeSql("create table dwd_trade_cart_add (\n"
                + "    id string,\n"
                + "    user_id string,\n"
                + "    sku_id string,\n"
                + "    sku_num string,\n"
                + "    ts bigint,\n"
                + "    PRIMARY KEY (id) NOT ENFORCED\n"
                + ")\n"
                + "WITH (\n"
                + "    'connector' = 'upsert-kafka',\n"
                + "    'topic' = 'dwd_trade_cart_add',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'key.format' = 'json',\n"
                + "    'value.format' = 'json'\n"
                + ")");

        cartInfoTable.executeInsert("dwd_trade_cart_add");
    }
}
