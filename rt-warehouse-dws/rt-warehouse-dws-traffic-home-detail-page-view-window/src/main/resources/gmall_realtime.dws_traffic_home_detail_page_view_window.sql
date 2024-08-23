drop table if exists gmall_realtime.dws_traffic_home_detail_page_view_window;
create table if not exists gmall_realtime.dws_traffic_home_detail_page_view_window
    (
    `stt`               DATETIME COMMENT '窗口起始时间',
    `edt`               DATETIME COMMENT '窗口结束时间',
    `cur_date`          DATE COMMENT '当天日期',
    `home_uv_ct`        BIGINT REPLACE COMMENT '首页独立访客数',
    `good_detail_uv_ct` BIGINT REPLACE COMMENT '商品详情页独立访客数'
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
