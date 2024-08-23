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
public class UserRegisterBean {

    /**
     * 窗口起始时间
     */
    private String stt;

    /**
     * 窗口终止时间
     */
    private String edt;

    /**
     * 当天日期
     */
    private String curDate;

    /**
     * 注册用户数
     */
    private Long registerCt;
}
