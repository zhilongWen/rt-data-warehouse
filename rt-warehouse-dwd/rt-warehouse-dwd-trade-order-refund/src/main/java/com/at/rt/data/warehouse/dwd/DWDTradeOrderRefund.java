package com.at.rt.data.warehouse.dwd;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author wenzhilong
 */
public class DWDTradeOrderRefund {

    public static void main(String[] args) {

        StreamTableEnvironment tableEnv = StreamExecEnvConf.builderStreamTableEnv(args);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        String sql = "-- 从 ODS_BASE_DB 主题中读取数据  创建动态表\n"
                + "CREATE TABLE topic_db (\n"
                + "    `database` string, \n"
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
                + "    'properties.group.id' = 'DWDTradeOrderRefund',  \n"
                + "    'scan.startup.mode' = 'earliest-offset',\n"
                + "    'format' = 'json'\n"
                + ")\n"
                + ";\n"
                + "\n"
                + "-- 从HBase中读取字典数据 创建动态表\n"
                + "CREATE TABLE base_dic (\n"
                + "    dic_code string,\n"
                + "    info ROW<dic_name string>,\n"
                + "    PRIMARY KEY (dic_code) NOT ENFORCED\n"
                + ")\n"
                + "WITH \n"
                + "(\n"
                + "    'connector' = 'hbase-2.2',\n"
                + "    'table-name' = 'gmall:dim_base_dic',\n"
                + "    'zookeeper.quorum' = 'hadoop102:2181',\n"
                + "    'lookup.async' = 'true',\n"
                + "    'lookup.cache' = 'PARTIAL',\n"
                + "    'lookup.partial-cache.max-rows' = '500',\n"
                + "    'lookup.partial-cache.expire-after-write' = '1 hour',\n"
                + "    'lookup.partial-cache.expire-after-access' = '1 hour'\n"
                + ")\n"
                + ";\n"
                + "\n"
                + "-- 过滤退单表数据 order_refund_info   insert\n"
                + "create temporary view order_refund_info as \n"
                + "select \n"
                + "    data['id'] id,\n"
                + "    data['user_id'] user_id,\n"
                + "    data['order_id'] order_id,\n"
                + "    data['sku_id'] sku_id,\n"
                + "    data['refund_type'] refund_type,\n"
                + "    data['refund_num'] refund_num,\n"
                + "    data['refund_amount'] refund_amount,\n"
                + "    data['refund_reason_type'] refund_reason_type,\n"
                + "    data['refund_reason_txt'] refund_reason_txt,\n"
                + "    data['create_time'] create_time,\n"
                + "    pt,\n"
                + "    ts \n"
                + "from topic_db \n"
                + "where `table`='order_refund_info' \n"
                + "and `type`='insert'\n"
                + ";\n"
                + "\n"
                + "-- 过滤订单表中的退单数据: order_info  update \n"
                + "create temporary view order_info as\n"
                + "select \n"
                + "    data['id'] id,\n"
                + "    data['province_id'] province_id,\n"
                + "    `old` \n"
                + "from topic_db \n"
                + "where `table`='order_info' \n"
                + "and `type`='update'\n"
                + "and `old`['order_status'] is not null \n"
                + "and `data`['order_status']='1005'\n"
                + ";\n"
                + "\n"
                + "-- sink\n"
                + "create table dwd_trade_order_refund(\n"
                + "    id string,\n"
                + "    user_id string,\n"
                + "    order_id string,\n"
                + "    sku_id string,\n"
                + "    province_id string,\n"
                + "    date_id string,\n"
                + "    create_time string,\n"
                + "    refund_type_code string,\n"
                + "    refund_type_name string,\n"
                + "    refund_reason_type_code string,\n"
                + "    refund_reason_type_name string,\n"
                + "    refund_reason_txt string,\n"
                + "    refund_num string,\n"
                + "    refund_amount string,\n"
                + "    ts bigint ,\n"
                + "    PRIMARY KEY (id) NOT ENFORCED \n"
                + ") WITH (\n"
                + "    'connector' = 'upsert-kafka',\n"
                + "    'topic' = 'dwd_trade_order_refund',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092',\n"
                + "    'key.format' = 'json',\n"
                + "    'value.format' = 'json' \n"
                + ")\n"
                + ";\n"
                + "\n"
                + "-- join: 普通的和 lookup join\n"
                + "insert into dwd_trade_order_refund\n"
                + "select \n"
                + "    ri.id,\n"
                + "    ri.user_id,\n"
                + "    ri.order_id,\n"
                + "    ri.sku_id,\n"
                + "    oi.province_id,\n"
                + "    date_format(ri.create_time,'yyyy-MM-dd') date_id,\n"
                + "    ri.create_time,\n"
                + "    ri.refund_type,\n"
                + "    dic1.info.dic_name,\n"
                + "    ri.refund_reason_type,\n"
                + "    dic2.info.dic_name,\n"
                + "    ri.refund_reason_txt,\n"
                + "    ri.refund_num,\n"
                + "    ri.refund_amount,\n"
                + "    ri.ts \n"
                + "from order_refund_info ri \n"
                + "join order_info oi \n"
                + "    on ri.order_id=oi.id \n"
                + "join base_dic for system_time as of ri.pt as dic1 on ri.refund_type=dic1.dic_code \n"
                + "join base_dic for system_time as of ri.pt as dic2 \n"
                + "    on ri.refund_reason_type=dic2.dic_code \n"
                + ";";

        StreamExecEnvConf.execSQL(tableEnv, sql);
    }
}
