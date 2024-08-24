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
public class TradePaymentBean {

    /**
     * 窗口起始时间
     */
    String stt;

    /**
     * 窗口终止时间
     */
    String edt;

    /**
     * 当天日期
     */
    String curDate;

    /**
     * 支付成功独立用户数
     */
    Long paymentSucUniqueUserCount;

    /**
     * 支付成功新用户数
     */
    Long paymentSucNewUserCount;

    /**
     * 时间戳
     */
    @JsonIgnore
    Long ts;
}
