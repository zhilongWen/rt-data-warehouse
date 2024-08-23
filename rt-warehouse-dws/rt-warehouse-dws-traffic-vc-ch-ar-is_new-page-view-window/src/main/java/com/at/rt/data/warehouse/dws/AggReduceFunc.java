package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.bean.TrafficPageViewBean;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author wenzhilong
 */
public class AggReduceFunc implements ReduceFunction<TrafficPageViewBean> {
    @Override
    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) {
        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
        return value1;
    }
}
