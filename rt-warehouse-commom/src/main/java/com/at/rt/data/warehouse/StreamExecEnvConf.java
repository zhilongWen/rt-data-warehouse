package com.at.rt.data.warehouse;

import com.at.rt.data.warehouse.utils.YamlUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static com.at.rt.data.warehouse.constant.FlinkConfConstant.*;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class StreamExecEnvConf {

    private static final Logger logger = LoggerFactory.getLogger(StreamExecEnvConf.class);

    public static StreamExecutionEnvironment builderStreamEnv(String[] args) {

        ParameterTool parameterTool = ParameterTool.fromArgs(args)
                .mergeWith(ParameterTool.fromSystemProperties())
                .mergeWith(ParameterTool.fromMap(YamlUtil.parseYaml()));

        logger.info("input args:{}", parameterTool.toMap());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 测试时可以设置 pipeline.operator-chaining = false，使 operator 不能 changing 再一起
        if (parameterTool.getBoolean(PIPELINE_OPERATOR_CHAINING)) {
            env.disableOperatorChaining();
        }

        // 重启策略
        if (StringUtils.isNotBlank(RESTART_STRATEGY + ".fix")) {
            env.getConfig().setRestartStrategy(
                    RestartStrategies.fixedDelayRestart(
                            parameterTool.getInt(RESTART_STRATEGY_FIX_ATTEMPTS, 3),
                            parameterTool.getLong(RESTART_STRATEGY_FIX_DELAY, 60000)
                    )
            );
        } else if (StringUtils.isNotBlank(RESTART_STRATEGY + ".fail")) {
            env.getConfig().setRestartStrategy(
                    RestartStrategies.failureRateRestart(
                            parameterTool.getInt(RESTART_STRATEGY_FAIL_FAILURERATE, 2),
                            Time.seconds(parameterTool.getLong(RESTART_STRATEGY_FAIL_FAILUREINTERVAL, 300000)),
                            Time.seconds(parameterTool.getLong(RESTART_STRATEGY_FAIL_DELAYINTERVAL, 60000))
                    )
            );
        } else {
            env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        }

        if (parameterTool.getBoolean(CHECKPOINT_ENABLE)) {
            setCheckpoint(env, parameterTool);
        }


        env.getConfig().setGlobalJobParameters(parameterTool);

        return env;
    }

    private static void setCheckpoint(StreamExecutionEnvironment env, ParameterTool parameterTool) {

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // start a checkpoint every 1 min
        env.enableCheckpointing(parameterTool.getLong(CHECKPOINT_INTERVAL, 60000));

        // advanced options:
        if ("AT_LEAST_ONCE".equals(parameterTool.get(CHECKPOINT_TYPE))) {
            // set mode to exactly-once (this is the default)
            checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

            if (parameterTool.getBoolean(CHECKPOINT_UNALIGNED)) {
                try {
                    // enables the unaligned checkpoints
                    checkpointConfig.enableUnalignedCheckpoints();
                } catch (UnsupportedOperationException e) {
                    Configuration config = new Configuration();
                    config.set(ExecutionCheckpointingOptions.FORCE_UNALIGNED, true);
                    env.configure(config);
                }
            }
        } else {
            // set mode to exactly-once (this is the default)
            checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        }

        // make sure 1 min of progress happen between checkpoints
        checkpointConfig.setMinPauseBetweenCheckpoints(parameterTool.getLong(CHECKPOINT_INTERVAL, 60000));

        // checkpoints have to complete within one minute, or are discarded
        checkpointConfig.setCheckpointTimeout(parameterTool.getLong(CHECKPOINT_TIMEOUT, 60000));

        // only two consecutive checkpoint failures are tolerated
        checkpointConfig.setTolerableCheckpointFailureNumber(parameterTool.getInt(CHECKPOINT_FAILURE_NUM, 3));

        // allow only one checkpoint to be in progress at the same time
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // enable externalized checkpoints which are retained
        // after job cancellation
        checkpointConfig.setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        // sets the checkpoint storage where checkpoint snapshots will be written
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, parameterTool.get(CHECKPOINT_STATE_STORAGE, "filesystem"));
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, parameterTool.get(CHECKPOINT_STATE_DIR, "hdfs://hadoop102:8020/rt/checkpoint/" + UUID.randomUUID()));
        env.configure(config);
    }
}
