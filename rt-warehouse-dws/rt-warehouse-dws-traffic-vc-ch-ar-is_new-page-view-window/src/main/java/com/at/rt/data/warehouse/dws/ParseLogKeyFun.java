package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.bean.TrafficPageViewBean;
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

/**
 * @author wenzhilong
 */
public class ParseLogKeyFun extends KeyedProcessFunction<String, JsonNode, TrafficPageViewBean> {

    private transient ValueState<String> lastVisitDateState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<String> valueStateDescriptor
                = new ValueStateDescriptor<>("lastVisitDateState", String.class);
        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                .updateTtlOnCreateAndWrite().neverReturnExpired().build());
        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void processElement(JsonNode value,
                               KeyedProcessFunction<String, JsonNode, TrafficPageViewBean>.Context ctx,
                               Collector<TrafficPageViewBean> out) throws Exception {

        JsonNode common = JSONWrapper.getJSONObject(value, "common");
        JsonNode page = JSONWrapper.getJSONObject(value, "page");

        // 从状态中获取当前设置上次访问日期
        String lastVisitDate = lastVisitDateState.value();
        //获取当前访问日期
        Long ts = JSONWrapper.getLong(value, "ts");
        String curVisitDate = DateUtil.tsToDate(ts);

        long uvCt = 0L;
        if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)) {
            uvCt = 1L;
            lastVisitDateState.update(curVisitDate);
        }

        String lastPageId = JSONWrapper.getString(page, "last_page_id");

        Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;

        out.collect(
                TrafficPageViewBean.builder()
                        .vc(JSONWrapper.getString(common, "vc"))
                        .ch(JSONWrapper.getString(common, "ch"))
                        .ar(JSONWrapper.getString(common, "ar"))
                        .isNew(JSONWrapper.getString(common, "is_new"))
                        .uvCt(uvCt)
                        .svCt(svCt)
                        .pvCt(1L)
                        .durSum(JSONWrapper.getLong(page, "during_time"))
                        .ts(ts)
                        .build()
        );
    }
}
