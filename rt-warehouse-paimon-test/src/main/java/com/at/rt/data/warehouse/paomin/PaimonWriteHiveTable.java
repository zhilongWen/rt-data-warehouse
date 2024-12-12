package com.at.rt.data.warehouse.paomin;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.concurrent.TimeUnit;

public class PaimonWriteHiveTable {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                // 任务失败的时间启动的间隔
                Time.of(5, TimeUnit.SECONDS),
                // 允许任务延迟时间 3s
                Time.of(5, TimeUnit.SECONDS))
        );

        env.setStateBackend(new FsStateBackend("file:///Users/wenzhilong/warehouse/space/rt-data-warehouse/ck"));
        env.getCheckpointConfig().setCheckpointInterval(1 * 60 * 1000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000L);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().enableUnalignedCheckpoints(false);

        DataStream<Row> input =
                env.fromElements(
                                Row.ofKind(RowKind.INSERT, "AA", 12L),
                                Row.ofKind(RowKind.INSERT, "BB", 5L),
                                Row.ofKind(RowKind.UPDATE_BEFORE, "CC", 12L),
                                Row.ofKind(RowKind.UPDATE_AFTER, "AA", 100L))
                        .returns(
                                Types.ROW_NAMED(
                                        new String[] {"word", "cnt"}, Types.STRING, Types.LONG));

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

        DataType inputType =
                DataTypes.ROW(
                        DataTypes.FIELD("word", DataTypes.STRING()),
                        DataTypes.FIELD("cnt", DataTypes.BIGINT()));

        FlinkSinkBuilder builder = new FlinkSinkBuilder(table).forRow(input, inputType);
        builder.build();

        env.execute();
    }
}
