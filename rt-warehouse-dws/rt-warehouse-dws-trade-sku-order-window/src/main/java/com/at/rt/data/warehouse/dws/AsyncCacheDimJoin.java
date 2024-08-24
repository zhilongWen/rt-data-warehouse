package com.at.rt.data.warehouse.dws;

import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.bean.TradeSkuOrderBean;
import com.at.rt.data.warehouse.utils.HBaseUtil;
import com.at.rt.data.warehouse.utils.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class AsyncCacheDimJoin {

    final static AsyncRetryStrategies.FixedDelayRetryStrategy fixedDelayRetryStrategy = new AsyncRetryStrategies
            // maxAttempts=3, fixedDelay=100ms
            .FixedDelayRetryStrategyBuilder(3, 100L)
            .ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
            .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
            .build();

    public static SingleOutputStreamOperator<TradeSkuOrderBean> dimJoinSku(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {

        return AsyncDataStream
                .unorderedWaitWithRetry(
                        stream,
                        new RichAsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

                            private AsyncConnection hbaseAsyncConn;
                            private StatefulRedisConnection<String, String> redisAsyncConn;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
                                redisAsyncConn = RedisUtil.getRedisAsyncConnection();
                            }

                            @Override
                            public void close() throws Exception {
                                HBaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);
                                RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
                            }


                            @Override
                            public void asyncInvoke(TradeSkuOrderBean orderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
                                //根据当前流中对象获取要关联的维度的主键
                                String skuId = orderBean.getSkuId();
                                //根据维度的主键到Redis中获取维度数据
                                JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, "dim_sku_info", skuId);

                                if (dimJsonObj != null) {
                                    //如果在Redis中找到了要关联的维度(缓存命中)。 直接将命中的维度作为结果返回
                                    System.out.println("~~~从Redis中获取维度数据~~~");
                                } else {
                                    //如果在Redis中没有找到要关联的维度，发送请求到HBase中查找
                                    dimJsonObj = HBaseUtil.readDimAsync(hbaseAsyncConn, "gmall", "dim_sku_info", skuId);
                                    if (dimJsonObj != null) {
                                        System.out.println("~~~从HBase中获取维度数据~~~");
                                        //将查找到的维度数据放到Redis中缓存起来，方便下次查询使用
                                        RedisUtil.writeDimAsync(redisAsyncConn, "dim_sku_info", skuId, dimJsonObj);
                                    } else {
                                        System.out.println("~~~维度数据没有找到~~~");
                                    }
                                }

                                //将维度对象相关的维度属性补充到流中对象上
                                if (dimJsonObj != null) {
                                    orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                                    orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                                    orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                                    orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                                }
                                //获取数据库交互的结果并发送给ResultFuture的回调函数，将关联后的数据传递到下游
                                resultFuture.complete(Collections.singleton(orderBean));
                            }
                        },
                        60,
                        TimeUnit.SECONDS,
                        3,
                        fixedDelayRetryStrategy
                );
    }

}
