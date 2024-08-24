package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.bean.TradeSkuOrderBean;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

/**
 * @author wenzhilong
 */
public class OrderDetailDistinctFunc extends KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean> {

    private transient ValueState<TradeSkuOrderBean> lastJsonObjState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<TradeSkuOrderBean> valueStateDescriptor
                = new ValueStateDescriptor<>("lastJsonObjState", TradeSkuOrderBean.class);
        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void processElement(TradeSkuOrderBean value,
                               KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean>.Context ctx,
                               Collector<TradeSkuOrderBean> out) throws Exception {

        if (value.getOriginalAmount() == null) {
            value.setOriginalAmount(BigDecimal.ZERO);
        }

        if (value.getActivityReduceAmount() == null) {
            value.setActivityReduceAmount(BigDecimal.ZERO);
        }

        if (value.getCouponReduceAmount() == null) {
            value.setCouponReduceAmount(BigDecimal.ZERO);
        }

        if (value.getOrderAmount() == null) {
            value.setOrderAmount(BigDecimal.ZERO);
        }

        value.setTs(value.getTs() * 1000L);

        // 从状态中获取上次接收到的数据
        TradeSkuOrderBean tradeSkuOrderBean = lastJsonObjState.value();

        if (tradeSkuOrderBean != null) {
            // 说明重复了 ，将已经发送到下游的数据(状态)，影响到度量值的字段进行取反再传递到下游

            tradeSkuOrderBean.setOriginalAmount(tradeSkuOrderBean.getOriginalAmount().negate());
            tradeSkuOrderBean.setActivityReduceAmount(tradeSkuOrderBean.getActivityReduceAmount().negate());
            tradeSkuOrderBean.setCouponReduceAmount(tradeSkuOrderBean.getCouponReduceAmount().negate());
            tradeSkuOrderBean.setOrderAmount(tradeSkuOrderBean.getOrderAmount().negate());

            out.collect(tradeSkuOrderBean);
        }

        lastJsonObjState.update(value);
        out.collect(value);
    }
}
