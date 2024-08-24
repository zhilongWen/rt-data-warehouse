package com.at.rt.data.warehouse.dws;

import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.bean.TradeSkuOrderBean;
import com.at.rt.data.warehouse.utils.HBaseUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.hadoop.hbase.client.Connection;

public class BaseDimJoin {

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinSku(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return stream
                // 关联sku维度
                .map(
                        new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

                            private transient Connection hbaseConn;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                hbaseConn = HBaseUtil.getHBaseConnection();
                            }

                            @Override
                            public void close() throws Exception {
                                HBaseUtil.closeHBaseConnection(hbaseConn);
                            }

                            @Override
                            public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                                //根据流中的对象获取要关联的维度的主键
                                String skuId = orderBean.getSkuId();
                                //根据维度的主键到Hbase维度表中获取对应的维度对象
                                //id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
                                JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, "gmall", "dim_sku_info", skuId, JSONObject.class);
                                //将维度对象相关的维度属性补充到流中对象上
                                if (skuInfoJsonObj != null) {
                                    orderBean.setSkuName(skuInfoJsonObj.getString("sku_name"));
                                    orderBean.setSpuId(skuInfoJsonObj.getString("spu_id"));
                                    orderBean.setCategory3Id(skuInfoJsonObj.getString("category3_id"));
                                    orderBean.setTrademarkId(skuInfoJsonObj.getString("tm_id"));
                                }
                                return orderBean;
                            }
                        });

    }

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinSpu(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return stream
                // 关联sku维度
                .map(
                        new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

                            private transient Connection hbaseConn;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                hbaseConn = HBaseUtil.getHBaseConnection();
                            }

                            @Override
                            public void close() throws Exception {
                                HBaseUtil.closeHBaseConnection(hbaseConn);
                            }

                            @Override
                            public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                                //根据流中的对象获取要关联的维度的主键
                                String spuId = orderBean.getSpuId();
                                //根据维度的主键到Hbase维度表中获取对应的维度对象
                                //id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
                                JSONObject dimJsonObj = HBaseUtil.getRow(hbaseConn, "gmall", "dim_spu_info", spuId, JSONObject.class);
                                //将维度对象相关的维度属性补充到流中对象上
                                if (dimJsonObj != null) {
                                    orderBean.setSpuName(dimJsonObj.getString("spu_name"));
                                }
                                return orderBean;
                            }
                        });
    }
}
