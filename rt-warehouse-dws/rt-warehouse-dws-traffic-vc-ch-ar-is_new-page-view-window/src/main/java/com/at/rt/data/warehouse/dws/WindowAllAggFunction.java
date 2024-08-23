package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.bean.TrafficPageViewBean;
import com.at.rt.data.warehouse.utils.DateUtil;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author wenzhilong
 */
public class WindowAllAggFunction implements
        WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> {
    @Override
    public void apply(Tuple4<String, String, String, String> value,
                      TimeWindow window,
                      Iterable<TrafficPageViewBean> input,
                      Collector<TrafficPageViewBean> out) {
        TrafficPageViewBean pageViewBean = input.iterator().next();
        String stt = DateUtil.tsToDateTime(window.getStart());
        String edt = DateUtil.tsToDateTime(window.getEnd());
        String curDate = DateUtil.tsToDate(window.getStart());
        pageViewBean.setStt(stt);
        pageViewBean.setEdt(edt);
        pageViewBean.setCurDate(curDate);
        out.collect(pageViewBean);
    }
}
