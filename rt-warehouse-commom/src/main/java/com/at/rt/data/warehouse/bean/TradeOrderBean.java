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
public class TradeOrderBean {

    /**
     * 窗口起始时间
     */
    String stt;

    /**
     * 窗口关闭时间
     */
    String edt;

    /**
     * 当天日期
     */
    String curDate;

    /**
     * 下单独立用户数
     */
    Long orderUniqueUserCount;

    /**
     * 下单新用户数
     */
    Long orderNewUserCount;

    /**
     * 时间戳
     */
    @JsonIgnore
    Long ts;
}
