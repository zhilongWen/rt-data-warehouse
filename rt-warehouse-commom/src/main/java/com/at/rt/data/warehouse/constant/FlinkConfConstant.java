package com.at.rt.data.warehouse.constant;

public class FlinkConfConstant {

    public static String ISLOCAL = "isLocal";

    public static String RESTART_STRATEGY = "restart.strategy";

    public static String RESTART_STRATEGY_FIX_ATTEMPTS = "restart.strategy.fix.attempts";

    public static String RESTART_STRATEGY_FIX_DELAY = "restart.strategy.fix.delay";

    public static String RESTART_STRATEGY_FAIL_FAILURERATE = "restart.strategy.fail.failureRate";

    public static String RESTART_STRATEGY_FAIL_FAILUREINTERVAL = "restart.strategy.fail.failureInterval";

    public static String RESTART_STRATEGY_FAIL_DELAYINTERVAL = "restart.strategy.fail.delayInterval";

    public static String PIPELINE_OPERATOR_CHAINING = "pipeline.operator-chaining";

    /**
     * checkpoint
     */

    /**
     * true or false
     */
    public static String CHECKPOINT_ENABLE = "checkpoint.enable";
    /**
     * 毫秒
     */
    public static String CHECKPOINT_TIMEOUT = "checkpoint.timeout";
    /**
     * 毫秒
     */
    public static String CHECKPOINT_INTERVAL = "checkpoint.interval";
    /**
     * AT_LEAST_ONCE EXACTLY_ONCE
     */
    public static String CHECKPOINT_TYPE = "checkpoint.type";
    /**
     * true or false
     * 非对齐模式下必须设置 EXACTLY_ONCE
     */
    public static String CHECKPOINT_UNALIGNED = "checkpoint.unaligned";
    /**
     * 失败次数
     */
    public static String CHECKPOINT_FAILURE_NUM = "checkpoint.failure-num";

    public static String CHECKPOINT_STATE_STORAGE = "checkpoint.state-storage";

    public static String CHECKPOINT_STATE_DIR = "checkpoint.state-dir";

}
