package com.at.rt.data.warehouse;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class MysqlCDCTest {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // start a checkpoint every 1000 ms
        env.enableCheckpointing(5000);

        // advanced options:

        // set mode to exactly-once (this is the default)
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // make sure 500 ms of progress happen between checkpoints
        checkpointConfig.setMinPauseBetweenCheckpoints(500);

        // checkpoints have to complete within one minute, or are discarded
        checkpointConfig.setCheckpointTimeout(60000);

        // only two consecutive checkpoint failures are tolerated
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        // allow only one checkpoint to be in progress at the same time
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // enable externalized checkpoints which are retained
        // after job cancellation
        checkpointConfig.setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        // enables the unaligned checkpoints
        checkpointConfig.enableUnalignedCheckpoints();

//         sets the checkpoint storage where checkpoint snapshots will be written
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://hadoop102:8020/rt/checkpoint/MysqlCDCTest");
        env.configure(config);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("root123")
                .databaseList("gmall")
                .tableList("gmall.user_info")
                .deserializer(new JsonDebeziumDeserializationSchema())
                // initial: 扫描一次全表，做一次快照，然后增量
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> kafkaSource = env
                .fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "mysql_source"
                ).setParallelism(1);


        // "op": c r u d
        kafkaSource.print();

        env.execute();
    }
}
