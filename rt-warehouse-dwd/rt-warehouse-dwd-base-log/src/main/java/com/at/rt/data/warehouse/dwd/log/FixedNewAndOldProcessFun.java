package com.at.rt.data.warehouse.dwd.log;

import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.utils.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author wenzhilong
 */
public class FixedNewAndOldProcessFun extends KeyedProcessFunction<String, JSONObject, JSONObject> {

    private transient ValueState<String> lastVisitDateState;

    @Override
    public void open(Configuration parameters) {

        ValueStateDescriptor<String> lastVisitDateStateDesc =
                new ValueStateDescriptor<>("lastVisitDateState", Types.STRING);
        lastVisitDateStateDesc.enableTimeToLive(
                StateTtlConfig.newBuilder(Time.seconds(10)).neverReturnExpired().updateTtlOnCreateAndWrite().build());
        lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDesc);
    }

    @Override
    public void processElement(JSONObject value,
                               KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx,
                               Collector<JSONObject> out) throws Exception {

        //获取is_new的值
        String isNew = value.getJSONObject("common").getString("is_new");
        //从状态中获取首次访问日期
        String lastVisitDate = lastVisitDateState.value();
        //获取当前访问日期
        Long ts = value.getLong("ts");
        String curVisitDate = DateUtil.tsToDate(ts);

        if ("1".equals(isNew)) {
            //如果is_new的值为1
            if (StringUtils.isEmpty(lastVisitDate)) {
                //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                lastVisitDateState.update(curVisitDate);
            } else {
                //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                if (!lastVisitDate.equals(curVisitDate)) {
                    isNew = "0";
                    value.getJSONObject("common").put("is_new", isNew);
                }
            }

        } else {
            //如果 is_new 的值为 0
            //	如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，
            // 日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
            if (StringUtils.isEmpty(lastVisitDate)) {
                String yesterDay = DateUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                lastVisitDateState.update(yesterDay);
            }
        }

        out.collect(value);
    }
}
