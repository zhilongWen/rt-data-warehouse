drop table if exists gmall_realtime.dws_trade_sku_order_window;
create table if not exists gmall_realtime.dws_trade_sku_order_window
(
    `stt`                    DATETIME COMMENT '窗口起始时间',
    `edt`                    DATETIME COMMENT '窗口结束时间',
    `cur_date`               DATE COMMENT '当天日期',
    `trademark_id`           SMALLINT COMMENT '品牌ID',
    `trademark_name`         CHAR(255) COMMENT '品牌名称',
    `category1_id`           SMALLINT COMMENT '一级品类ID',
    `category1_name`         CHAR(128) COMMENT '一级品类名称',
    `category2_id`           SMALLINT COMMENT '二级品类ID',
    `category2_name`         CHAR(128) COMMENT '二级品类名称',
    `category3_id`           SMALLINT COMMENT '三级品类ID',
    `category3_name`         CHAR(128) COMMENT '三级品类名称',
    `sku_id`                 INT COMMENT 'SKU_ID',
    `sku_name`               CHAR(255) COMMENT 'SKU名称',
    `spu_id`                 INT COMMENT 'SPU_ID',
    `spu_name`               CHAR(255) COMMENT 'SPU名称',
    `original_amount`        DECIMAL(16, 2) REPLACE COMMENT '原始金额',
    `activity_reduce_amount` DECIMAL(16, 2) REPLACE COMMENT '活动减免金额',
    `coupon_reduce_amount`   DECIMAL(16, 2) REPLACE COMMENT '优惠券减免金额',
    `order_amount`           DECIMAL(16, 2) REPLACE COMMENT '下单金额'
)
engine = olap
aggregate key (`stt`,`edt`,`cur_date`,`trademark_id`,`trademark_name`,`category1_id`,`category1_name`,`category2_id`,`category2_name`,`category3_id`,`category3_name`,`sku_id`,`sku_name`,`spu_id`,`spu_name`)
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
