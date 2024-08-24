package com.at.rt.data.warehouse.bean;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;

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
public class TradeSkuOrderBean implements Serializable {

    @JsonProperty(value = "id", access = JsonProperty.Access.WRITE_ONLY)
    private String orderDetailId;

    /**
     * 窗口起始时间
     */
    @JsonProperty( access = JsonProperty.Access.READ_ONLY)
    private String stt;

    /**
     * 窗口结束时间
     */
    @JsonProperty( access = JsonProperty.Access.READ_ONLY)
    private String edt;

    /**
     * 当天日期
     */
    @JsonProperty( access = JsonProperty.Access.READ_ONLY)
    private String curDate;

    /**
     * 品牌 ID
     */
    @JsonProperty( access = JsonProperty.Access.READ_ONLY)
    private String trademarkId;

    /**
     * 品牌名称
     */
    @JsonProperty( access = JsonProperty.Access.READ_ONLY)
    private String trademarkName;

    /**
     * 一级品类 ID
     */
    @JsonProperty( access = JsonProperty.Access.READ_ONLY)
    private String category1Id;

    /**
     * 一级品类名称
     */
    @JsonProperty( access = JsonProperty.Access.READ_ONLY)
    private String category1Name;

    /**
     * 二级品类 ID
     */
    @JsonProperty( access = JsonProperty.Access.READ_ONLY)
    private String category2Id;

    /**
     * 二级品类名称
     */
    @JsonProperty( access = JsonProperty.Access.READ_ONLY)
    private String category2Name;

    /**
     * 三级品类 ID
     */
    @JsonProperty( access = JsonProperty.Access.READ_ONLY)
    private String category3Id;

    /**
     * 三级品类名称
     */
    @JsonProperty( access = JsonProperty.Access.READ_ONLY)
    private String category3Name;

    /**
     * sku_id
     */
    @JsonProperty("sku_id")
    private String skuId;

    /**
     * sku 名称
     */
    @JsonProperty("sku_name")
    private String skuName;

    /**
     * spu_id
     */
    private String spuId;

    /**
     * spu 名称
     */
    private String spuName;

    /**
     * 原始金额
     */
    @JsonProperty("split_original_amount")
    @JsonSerialize(using = BigDecimalDefaultZeroSerializer.class)
    @JsonDeserialize(using = BigDecimalDefaultZeroDeserializer.class)
    private BigDecimal originalAmount;

    /**
     * 活动减免金额
     */
    @JsonProperty("split_activity_amount")
    @JsonSerialize(using = BigDecimalDefaultZeroSerializer.class)
    @JsonDeserialize(using = BigDecimalDefaultZeroDeserializer.class)
    private BigDecimal activityReduceAmount;

    /**
     * 优惠券减免金额
     */
    @JsonProperty("split_coupon_amount")
    @JsonSerialize(using = BigDecimalDefaultZeroSerializer.class)
    @JsonDeserialize(using = BigDecimalDefaultZeroDeserializer.class)
    private BigDecimal couponReduceAmount;

    /**
     * 下单金额
     */
    @JsonProperty("split_total_amount")
    @JsonSerialize(using = BigDecimalDefaultZeroSerializer.class)
    @JsonDeserialize(using = BigDecimalDefaultZeroDeserializer.class)
    private BigDecimal orderAmount;

    /**
     * 时间戳
     */
    @JsonProperty(value = "ts", access = JsonProperty.Access.WRITE_ONLY)
    private long ts;
}
