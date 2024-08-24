package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import com.at.rt.data.warehouse.bean.TradeSkuOrderBean;
import com.at.rt.data.warehouse.utils.DateUtil;
import com.at.rt.data.warehouse.utils.FlinkKafkaUtil;
import com.at.rt.data.warehouse.utils.JSONWrapper;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

/**
 *
 * 交易域SKU粒度下单各窗口汇总表
 *
 * @author wenzhilong
 */
public class DWSTradeSkuOrderWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);

        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        // {"id":"13518","order_id":"5079","user_id":"351","sku_id":"33","sku_name":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 粉邂逅淡香水35ml","province_id":"20","activity_id":null,"activity_rule_id":null,"coupon_id":null,"date_id":"2024-08-22","create_time":"2024-08-22 23:53:51","sku_num":"3","split_original_amount":"1464.0000","split_activity_amount":null,"split_coupon_amount":null,"split_total_amount":"1464.0","ts":1724342031}
        SingleOutputStreamOperator<TradeSkuOrderBean> skuAggStream = env
                .fromSource(
                        FlinkKafkaUtil.getConsumer(parameterTool, "dwd_trade_order_detail"),
                        WatermarkStrategy.noWatermarks(),
                        "dwd_trade_order_detail"
                )
                //  1.对流中数据类型进行转
                .map(value -> JSONWrapper.parseObject(value, TradeSkuOrderBean.class))
                // 2.按照唯一键(订单明细的id)进行分组
                .keyBy(TradeSkuOrderBean::getOrderDetailId)
                // 3.去重 状态 + 抵消    优点：时效性好    缺点：如果出现重复，需要向下游传递3条数据(数据膨胀)
                .process(new OrderDetailDistinctFunc())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withIdleness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<TradeSkuOrderBean>) (element, recordTimestamp) -> element.getTs()
                                )
                )
                .keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                                value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                                value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context,
                                                Iterable<TradeSkuOrderBean> elements,
                                                Collector<TradeSkuOrderBean> out) throws Exception {
                                TradeSkuOrderBean orderBean = elements.iterator().next();
                                TimeWindow window = context.window();
                                String stt = DateUtil.tsToDateTime(window.getStart());
                                String edt = DateUtil.tsToDateTime(window.getEnd());
                                String curDate = DateUtil.tsToDate(window.getStart());
                                orderBean.setStt(stt);
                                orderBean.setEdt(edt);
                                orderBean.setCurDate(curDate);
                                out.collect(orderBean);
                            }
                        }
                );

//        // 关联 sku 维度
//        SingleOutputStreamOperator<TradeSkuOrderBean> map = BaseDimJoin.dimJoinSku(skuAggStream);
//        // 关联 spu 维度
//        map = BaseDimJoin.dimJoinSpu(map);

        // 关联 sku 维度
//        SingleOutputStreamOperator<TradeSkuOrderBean> map = CacheDimJoin.dimJoinSku(skuAggStream);
//         关联 spu 维度
//        map = CacheDimJoin.dimJoinSpu(map);

//        // 关联 sku 维度
//        SingleOutputStreamOperator<TradeSkuOrderBean> map = TemplateCacheDimJoin.dimJoinSku(skuAggStream);
////         关联 spu 维度
//        map = TemplateCacheDimJoin.dimJoinSpu(map);

        // 关联 sku 维度
//        SingleOutputStreamOperator<TradeSkuOrderBean> map = AsyncCacheDimJoin.dimJoinSku(skuAggStream);
//        map.print();

        //异步IO + 模板

        // 关联 sku 维度
        SingleOutputStreamOperator<TradeSkuOrderBean> map = AsyncTemplateCacheDimJoin.dimJoinSku(skuAggStream);
        // 关联 spu 维度
        map = AsyncTemplateCacheDimJoin.dimJoinSpu(map);
        // 关联tm维度
        map = AsyncTemplateCacheDimJoin.dimJoinTm(map);
        // 关联category3维度
        map = AsyncTemplateCacheDimJoin.dimJoinCat3(map);
        // 关联category2维度
        map = AsyncTemplateCacheDimJoin.dimJoinCat2(map);
        // 关联category1维度
        map = AsyncTemplateCacheDimJoin.dimJoinCat1(map);

        Properties props = new Properties();
        props.setProperty("format", "json");
        // 每行一条 json 数据
        props.setProperty("read_json_by_line", "true");

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                // 设置 doris 的连接参数
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes("10.211.55.102:7030")
                        .setTableIdentifier("gmall_realtime.dws_trade_sku_order_window")
                        .setUsername("root")
                        .setPassword("root123")
                        .build())
                // 执行参数
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        // stream-load 导入的时候的 label 前缀
                        //.setLabelPrefix("doris-label")
                        // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .disable2PC()
                        .setDeletable(false)
                        // 用于缓存stream load数据的缓冲条数: 默认 3
                        .setBufferCount(3)
                        //用于缓存stream load数据的缓冲区大小: 默认 1M
                        .setBufferSize(1024 * 1024)
                        .setMaxRetries(3)
                        // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .setStreamLoadProp(props)
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();

        map.map(JSONWrapper::toJSONStringSnake)
                .sinkTo(sink);

        env.execute();
    }
}
