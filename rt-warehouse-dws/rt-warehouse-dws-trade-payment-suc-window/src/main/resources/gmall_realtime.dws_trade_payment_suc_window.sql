drop table if exists gmall_realtime.dws_trade_payment_suc_window;
create table if not exists gmall_realtime.dws_trade_payment_suc_window
    (
        `stt`                           DATETIME COMMENT '窗口起始时间',
        `edt`                           DATETIME COMMENT '窗口结束时间',
        `cur_date`                      DATE COMMENT '当天日期',
        `payment_suc_unique_user_count` BIGINT REPLACE COMMENT '支付成功独立用户数',
        `payment_suc_new_user_count`    BIGINT REPLACE COMMENT '支付成功新用户数'
)
engine = olap
aggregate key (`stt`,`edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "30",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "10"
);

