package com.at.rt.data.warehouse.dws;

import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.bean.TradeSkuOrderBean;
import com.at.rt.data.warehouse.utils.HBaseUtil;
import com.at.rt.data.warehouse.utils.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * @author wenzhilong
 */
public class CacheDimJoin {

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinSku(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return stream
                .map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

                    private Connection hbaseConn;
                    private Jedis jedis;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                        RedisUtil.closeJedis(jedis);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据维度的主键，先到Redis中查询维度
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, "dim_sku_info", skuId);
                        if (dimJsonObj != null) {
                            //如果在Redis中找到了对应的维度数据(缓存命中)，直接作为查询结果返回
                            System.out.println("~~~从Redis中查询维度数据~~~");
                        } else {
                            //如果在Redis中没有找到对应的维度数据，发送请求到HBase中查询对应的维度
                            dimJsonObj = HBaseUtil.getRow(hbaseConn, "gmall", "dim_sku_info", skuId, JSONObject.class);
                            if (dimJsonObj != null) {
                                //并将查询出来的维度放到Redis中缓存起来
                                System.out.println("~~~从HBase中查询维度数据~~~");
                                RedisUtil.writeDim(jedis, "dim_sku_info", skuId, dimJsonObj);
                            } else {
                                System.out.println("~~~没有找到要关联的维度~~~");
                            }
                        }

                        //将维度对象相关的维度属性补充到流中对象上
                        if (dimJsonObj != null) {
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }

                        return orderBean;
                    }
                });
    }

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinSpu(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return stream
                .map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

                    private Connection hbaseConn;
                    private Jedis jedis;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                        RedisUtil.closeJedis(jedis);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据维度的主键，先到Redis中查询维度
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, "dim_spu_info", skuId);
                        if (dimJsonObj != null) {
                            //如果在Redis中找到了对应的维度数据(缓存命中)，直接作为查询结果返回
                            System.out.println("~~~从Redis中查询维度数据~~~");
                        } else {
                            //如果在Redis中没有找到对应的维度数据，发送请求到HBase中查询对应的维度
                            dimJsonObj = HBaseUtil.getRow(hbaseConn, "gmall", "dim_spu_info", skuId, JSONObject.class);
                            if (dimJsonObj != null) {
                                //并将查询出来的维度放到Redis中缓存起来
                                System.out.println("~~~从HBase中查询维度数据~~~");
                                RedisUtil.writeDim(jedis, "dim_spu_info", skuId, dimJsonObj);
                            } else {
                                System.out.println("~~~没有找到要关联的维度~~~");
                            }
                        }

                        //将维度对象相关的维度属性补充到流中对象上
                        if (dimJsonObj != null) {
                            orderBean.setSpuName(dimJsonObj.getString("spu_name"));
                        }

                        return orderBean;
                    }
                });
    }

}
