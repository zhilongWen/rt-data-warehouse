package com.at.rt.data.warehouse;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * Hello world!
 */
public class MainAPP {

    public StreamExecutionEnvironment start(String[] args,String ckPath) {

        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration configuration = new Configuration();
        configuration.setString("rest.port", "9099");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

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
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://hadoop102:8020/rt/checkpoint/" + ckPath);
        env.configure(config);

        return env;
    }
}
