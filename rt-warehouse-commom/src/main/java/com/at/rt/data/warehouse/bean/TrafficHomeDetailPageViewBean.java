package com.at.rt.data.warehouse.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
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
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TrafficHomeDetailPageViewBean {

    /**
     * 窗口起始时间
     */
    String stt;

    /**
     * 窗口结束时间
     */
    String edt;

    /**
     * 当天日期
     */
    String curDate;

    /**
     * 首页独立访客数
     */
    Long homeUvCt;

    /**
     * 商品详情页独立访客数
     */
    Long goodDetailUvCt;

    /**
     * 时间戳
     */
    @JsonIgnore
    Long ts;
}
