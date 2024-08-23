package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.bean.TrafficHomeDetailPageViewBean;
import com.at.rt.data.warehouse.utils.DateUtil;
import com.at.rt.data.warehouse.utils.JSONWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author wenzhilong
 */
public class ParsePageLogFunc extends KeyedProcessFunction<String, JsonNode, TrafficHomeDetailPageViewBean> {

    private transient ValueState<String> homeLastVisitDateState;
    private transient ValueState<String> detailLastVisitDateState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<String> homeValueStateDescriptor
                = new ValueStateDescriptor<>("homeLastVisitDateState", String.class);
        homeValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
        homeLastVisitDateState = getRuntimeContext().getState(homeValueStateDescriptor);

        ValueStateDescriptor<String> detailValueStateDescriptor
                = new ValueStateDescriptor<>("detailLastVisitDateState", String.class);
        detailValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
        detailLastVisitDateState = getRuntimeContext().getState(detailValueStateDescriptor);
    }

    @Override
    public void processElement(JsonNode value,
                               KeyedProcessFunction<String, JsonNode, TrafficHomeDetailPageViewBean>.Context ctx,
                               Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

        String pageId = JSONWrapper.getString(value, Arrays.asList("page", "page_id"));

        Long ts = value.get("ts").asLong();
        String curVisitDate = DateUtil.tsToDate(ts);
        long homeUvCt = 0L;
        long detailUvCt = 0L;

        if ("home".equals(pageId)) {
            //获取首页的上次访问日期
            String homeLastVisitDate = homeLastVisitDateState.value();
            if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                homeUvCt = 1L;
                homeLastVisitDateState.update(curVisitDate);
            }
        } else {
            //获取详情页的上次访问日期
            String detailLastVisitDate = detailLastVisitDateState.value();
            if (StringUtils.isEmpty(detailLastVisitDate) || !detailLastVisitDate.equals(curVisitDate)) {
                detailUvCt = 1L;
                detailLastVisitDateState.update(curVisitDate);
            }
        }

        if (homeUvCt != 0L || detailUvCt != 0L) {
            out.collect(new TrafficHomeDetailPageViewBean(
                    "", "", "", homeUvCt, detailUvCt, ts
            ));
        }
    }
}
