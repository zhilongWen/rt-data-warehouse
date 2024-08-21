package com.at.rt.data.warehouse.dwd;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 退款成功事实表
 *
 * @author wenzhilong
 */
public class DWDTradeRefundPaySucDetail {

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
                + "    'properties.group.id' = 'DwdTradeOrderDetail',  \n"
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
                + "-- 过滤退款成功表数据\n"
                + "create temporary view refund_payment as\n"
                + "select \n"
                + "    data['id'] id,\n"
                + "    data['order_id'] order_id,\n"
                + "    data['sku_id'] sku_id,\n"
                + "    data['payment_type'] payment_type,\n"
                + "    data['callback_time'] callback_time,\n"
                + "    data['total_amount'] total_amount,\n"
                + "    pt, \n"
                + "    ts \n"
                + "from topic_db \n"
                + "where `table`='refund_payment' \n"
                + "and `type`='update' \n"
                + "and `old`['refund_status'] is not null \n"
                + "and `data`['refund_status']='1602'\n"
                + ";\n"
                + "\n"
                + "-- 过滤退单表中的退单成功的数据\n"
                + "create temporary view order_refund_info as\n"
                + "select \n"
                + "    data['order_id'] order_id,\n"
                + "    data['sku_id'] sku_id,\n"
                + "    data['refund_num'] refund_num \n"
                + "from topic_db \n"
                + "where `table`='order_refund_info' \n"
                + "and `type`='update' \n"
                + "and `old`['refund_status'] is not null \n"
                + "and `data`['refund_status']='0705'\n"
                + ";\n"
                + "\n"
                + "-- 过滤订单表中的退款成功的数据\n"
                + "create temporary view order_info as\n"
                + "select \n"
                + "    data['id'] id,\n"
                + "    data['user_id'] user_id,\n"
                + "    data['province_id'] province_id \n"
                + "from topic_db \n"
                + "where `table`='order_info' \n"
                + "and `type`='update' \n"
                + "and `old`['order_status'] is not null \n"
                + "and `data`['order_status']='1006'\n"
                + ";\n"
                + "\n"
                + "-- sink\n"
                + "create table dwd_trade_refund_payment_success(\n"
                + "    id string,\n"
                + "    user_id string,\n"
                + "    order_id string,\n"
                + "    sku_id string,\n"
                + "    province_id string,\n"
                + "    payment_type_code string,\n"
                + "    payment_type_name string,\n"
                + "    date_id string,\n"
                + "    callback_time string,\n"
                + "    refund_num string,\n"
                + "    refund_amount string,\n"
                + "    ts bigint ,\n"
                + "    PRIMARY KEY (id) NOT ENFORCED \n"
                + ")WITH (\n"
                + "    'connector' = 'upsert-kafka',\n"
                + "    'topic' = 'dwd_trade_refund_payment_success',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092',\n"
                + "    'key.format' = 'json',\n"
                + "    'value.format' = 'json'\n"
                + ")\n"
                + ";\n"
                + "\n"
                + "-- 4 张表的 join\n"
                + "insert into dwd_trade_refund_payment_success \n"
                + "select \n"
                + "    rp.id,\n"
                + "    oi.user_id,\n"
                + "    rp.order_id,\n"
                + "    rp.sku_id,\n"
                + "    oi.province_id,\n"
                + "    rp.payment_type,\n"
                + "    dic.info.dic_name payment_type_name,\n"
                + "    date_format(rp.callback_time,'yyyy-MM-dd') date_id,\n"
                + "    rp.callback_time,\n"
                + "    ori.refund_num,\n"
                + "    rp.total_amount,\n"
                + "    rp.ts \n"
                + "from refund_payment rp \n"
                + "join order_refund_info ori \n"
                + "    on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id \n"
                + "join order_info oi \n"
                + "    on rp.order_id=oi.id \n"
                + "join base_dic for system_time as of rp.pt as dic on rp.payment_type=dic.dic_code\n"
                + ";";

        StreamExecEnvConf.execSQL(tableEnv, sql);
    }
}
