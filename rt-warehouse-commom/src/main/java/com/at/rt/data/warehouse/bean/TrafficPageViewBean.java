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
public class TrafficPageViewBean {

    /**
     * 窗口起始时间
     */
    private String stt;

    /**
     * 窗口结束时间
     */
    private String edt;

    /**
     * 当天日期
     */
    private String curDate;

    /**
     * app 版本号
     */
    private String vc;

    /**
     * 渠道
     */
    private String ch;

    /**
     * 地区
     */
    private String ar;

    /**
     * 新老访客状态标记
     */
    private String isNew;

    /**
     * 独立访客数
     */
    private Long uvCt;

    /**
     * 会话数
     */
    private Long svCt;

    /**
     * 页面浏览数
     */
    private Long pvCt;

    /**
     * 累计访问时长
     */
    private Long durSum;

    /**
     * 时间戳
     * 要不要序列化这个字段
     */
    @JsonIgnore
    private Long ts;
}
