package com.at.rt.data.warehouse.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Set;

/**
 * @author wenzhilong
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
@EqualsAndHashCode
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TradeProvinceOrderBean implements Serializable {

    /**
     * 窗口起始时间
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private String stt;

    /**
     * 窗口结束时间
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private String edt;

    /**
     * 当天日期
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private String curDate;

    /**
     * 省份 ID
     */
    @JsonProperty("province_id")
    private String provinceId;

    /**
     * 省份名称
     */
    @Builder.Default
    private String provinceName = "";

    /**
     * 累计下单次数
     */
    private Long orderCount;

    /**
     * 累计下单金额
     */
    private BigDecimal orderAmount;

    /**
     * 时间戳
     */
    @JsonProperty(value = "ts", access = JsonProperty.Access.WRITE_ONLY)
    private Long ts;

    @JsonProperty(value = "id", access = JsonProperty.Access.WRITE_ONLY)
    private String orderDetailId;

    @JsonIgnore
    private Set<String> orderIdSet;
}
