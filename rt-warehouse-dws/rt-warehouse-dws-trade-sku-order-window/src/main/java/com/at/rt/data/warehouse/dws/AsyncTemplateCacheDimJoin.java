package com.at.rt.data.warehouse.dws;

import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.bean.TradeSkuOrderBean;
import com.at.rt.data.warehouse.function.DimAsyncFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.concurrent.TimeUnit;

/**
 * @author wenzhilong
 */
public class AsyncTemplateCacheDimJoin {


    private static SingleOutputStreamOperator<TradeSkuOrderBean> join(SingleOutputStreamOperator<TradeSkuOrderBean> stream, DimAsyncFunction<TradeSkuOrderBean> function) {
        return AsyncDataStream.unorderedWaitWithRetry(
                stream,
                function,
                60,
                TimeUnit.SECONDS,
                3,
                AsyncCacheDimJoin.fixedDelayRetryStrategy
        );
    }

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinSku(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return join(
                stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
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
                }
        );
    }

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinSpu(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return join(
                stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
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
                }
        );
    }

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinTm(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return join(
                stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setTrademarkName(dimJsonObj.getString("tm_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getTrademarkId();
                    }
                }
        );
    }

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinCat3(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return join(
                stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory3Name(dim.getString("name"));
                        bean.setCategory2Id(dim.getString("category2_id"));
                    }
                }
        );
    }

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinCat2(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return join(
                stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory2Name(dim.getString("name"));
                        bean.setCategory1Id(dim.getString("category1_id"));
                    }
                }
        );
    }

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinCat1(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return join(
                stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory1Name(dim.getString("name"));
                    }
                }
        );
    }
}
