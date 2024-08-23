drop table if exists gmall_realtime.dws_traffic_vc_ch_ar_is_new_page_view_window;
create table if not exists gmall_realtime.dws_traffic_vc_ch_ar_is_new_page_view_window
(
    `stt`      DATETIME COMMENT '窗口起始时间',
    `edt`      DATETIME COMMENT '窗口结束时间',
    `cur_date` DATE COMMENT '当天日期',
    `vc`       VARCHAR(256) COMMENT '版本号',
    `ch`       VARCHAR(256) COMMENT '渠道',
    `ar`       TINYINT COMMENT '地区',
    `is_new`   TINYINT COMMENT '新老访客状态标记',
    `uv_ct`    BIGINT REPLACE COMMENT '独立访客数',
    `sv_ct`    BIGINT REPLACE COMMENT '会话数',
    `pv_ct`    BIGINT REPLACE COMMENT '页面浏览数',
    `dur_sum`  BIGINT REPLACE COMMENT '累计访问时长'
)
engine = olap aggregate key (`stt`,`edt`,`cur_date`,`vc`,`ch`,`ar`,`is_new`)
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
