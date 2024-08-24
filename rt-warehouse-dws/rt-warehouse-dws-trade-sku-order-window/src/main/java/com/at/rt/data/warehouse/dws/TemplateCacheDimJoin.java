package com.at.rt.data.warehouse.dws;

import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.bean.TradeSkuOrderBean;
import com.at.rt.data.warehouse.function.DimMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @author wenzhilong
 */
public class TemplateCacheDimJoin {

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinSku(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return stream
                .map(new DimMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                });
    }

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinSpu(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return stream
                .map(new DimMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSpuName(dimJsonObj.getString("spu_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSpuId();
                    }
                });
    }
}
