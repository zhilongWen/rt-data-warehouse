package com.at.rt.data.warehouse.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

public class CDCUtil {

    public static MySqlSource<String> getMysqlSource(String hostname, int port, String username, String passwd, String database, String table) {
        return MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(passwd)
                .databaseList(database)
                .tableList(database + "." + table)
                .deserializer(new JsonDebeziumDeserializationSchema())
                // initial: 扫描一次全表，做一次快照，然后增量
                .startupOptions(StartupOptions.initial())
                .build();
    }

    public static MySqlSource<String> getMysqlSource(String database, String table) {
        return MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3360)
                .username("root")
                .password("root123")
                .databaseList(database)
                .tableList(String.format("%s.%s",database,table))
                .deserializer(new JsonDebeziumDeserializationSchema())
                // initial: 扫描一次全表，做一次快照，然后增量
                .startupOptions(StartupOptions.initial())
                .build();
    }

}
