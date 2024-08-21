package com.at.rt.data.warehouse.dwd;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class Test {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("root123")
                .databaseList("rt_warehouse_conf_db")
                .tableList("rt_warehouse_conf_db.table_process_dwd_conf")
                .deserializer(new JsonDebeziumDeserializationSchema())
                // initial: 扫描一次全表，做一次快照，然后增量
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();

        env
                .fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "table_process_dwd_conf"
                )
                .print();


        env.execute();
    }
}
