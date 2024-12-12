package com.at.rt.data.warehouse.paomin;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;


public class PaimonReadHiveTable {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get table from catalog
        Options catalogOptions = new Options();
        catalogOptions.set("type", "paimon");
        catalogOptions.set("metastore", "hive");
        catalogOptions.set("uri", "thrift://10.211.55.102:9083");
        catalogOptions.set("hive-conf-dir", "/Users/wenzhilong/warehouse/space/rt-data-warehouse/conf");
        catalogOptions.set("hadoop-conf-dir", "/Users/wenzhilong/warehouse/space/rt-data-warehouse/conf");
        catalogOptions.set("table-default.hive.strict.managed.tables", "false");
        catalogOptions.set("table-default.hive.create.as.insert.only", "false");
        catalogOptions.set("table-default.metastore.create.as.acid", "false");
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        Table table = catalog.getTable(Identifier.create("default", "word_count"));

        FlinkSourceBuilder builder = new FlinkSourceBuilder(table).env(env);
        DataStream<Row> dataStream = builder.buildForRow();
        dataStream.executeAndCollect().forEachRemaining(System.out::println);

        env.execute();
    }
}
