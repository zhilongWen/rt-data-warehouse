drop table if exists gmall_realtime.dws_trade_province_order_window;
create table if not exists gmall_realtime.dws_trade_province_order_window
(
    `stt`           DATETIME COMMENT '窗口起始时间',
    `edt`           DATETIME COMMENT '窗口结束时间',
    `cur_date`      DATE COMMENT '当天日期',
    `province_id`   TINYINT COMMENT '省份ID',
    `province_name` CHAR(128) COMMENT '省份名称',
    `order_count`   BIGINT REPLACE COMMENT '累计下单次数',
    `order_amount`  DECIMAL(16, 2) REPLACE COMMENT '累计下单金额'
)
engine = olap
aggregate key (`stt`, `edt`,`cur_date`,`province_id`,`province_name`)
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
