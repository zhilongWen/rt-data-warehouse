package com.at.rt.data.warehouse.bean;

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
public class CartAddUuBean {

    /**
     * 窗口起始时间
     */
    String stt;

    /**
     * 窗口闭合时间
     */
    String edt;

    /**
     * 当天日期
     */
    String curDate;

    /**
     * 加购独立用户数
     */
    Long cartAddUuCt;

}
