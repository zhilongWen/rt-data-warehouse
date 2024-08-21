package com.at.rt.data.warehouse.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author wenzhilong
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableProcessConfDwd {

    /**
     * 来源表名
     */
    @JsonProperty(value = "source_table")
    private String sourceTable;

    /**
     * 来源类型
     */
    @JsonProperty(value = "source_type")
    private String sourceType;

    /**
     * 目标表名
     */
    @JsonProperty(value = "sink_table")
    private String sinkTable;

    /**
     * 输出字段
     */
    @JsonProperty(value = "sink_columns")
    private String sinkColumns;

    /**
     * 配置表操作类型
     */
    @JsonIgnore
    private String op;
}
