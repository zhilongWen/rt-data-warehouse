package com.at.rt.data.warehouse.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author wenzhilong
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TableProcessDim {

    /**
     * 来源表名
     */
    private String sourceTable;

    /**
     * 目标表名
     */
    private String sinkTable;

    /**
     * 输出字段
     */
    private String sinkColumns;

    /**
     * 数据到 hbase 的列族
     */
    private String sinkFamily;

    /**
     * sink到 hbase 的时候的主键字段
     */
    private String sinkRowKey;

    /**
     * 配置表操作类型
     */
    private String op;
}
