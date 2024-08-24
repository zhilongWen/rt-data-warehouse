package com.at.rt.data.warehouse;

import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.bean.TableProcessDim;
import com.at.rt.data.warehouse.utils.FlinkKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p>
 * 开发流程
 * 基本环境准备
 * 检查点相关的设置
 * 从kafka主题中读取数据
 * 对流中数据进行类型转换并ETL     jsonStr->jsonObj
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * 使用FlinkCDC读取配置表中的配置信息
 * 对读取到的配置流数据进行类型转换    jsonStr->实体类对象
 * 根据当前配置信息到HBase中执行建表或者删除操作
 * op=d        删表
 * op=c、r     建表
 * op=u        先删表，再建表
 * 对配置流数据进行广播---broadcast
 * 关联主流业务数据以及广播流配置数据---connect
 * 对关联后的数据进行处理---process
 * new TableProcessFunction extends BroadcastProcessFunction{
 * open:将配置信息预加载到程序中，避免主流数据先到，广播流数据后到，丢失数据的情况
 * processElement:对主流数据的处理
 * 获取操作的表的表名
 * 根据表名到广播状态中以及configMap中获取对应的配置信息，如果配置信息不为空，说明是维度，将维度数据发送到下游
 * Tuple2<dataJsonObj,配置对象>
 * 在向下游发送数据前，过滤掉了不需要传递的属性
 * 在向下游发送数据前，补充操作类型
 * processBroadcastElement:对广播流数据进行处理
 * op =d  将配置信息从广播状态以及configMap中删除掉
 * op!=d  将配置信息放到广播状态以及configMap中
 * }
 * 将流中数据同步到HBase中
 * class HBaseSinkFunction extends RichSinkFunction{
 * invoke:
 * type="delete" : 从HBase表中删除数据
 * type!="delete" :向HBase表中put数据
 * }
 * 优化：抽取FlinkSourceUtil工具类
 * 抽取TableProcessFunction以及HBaseSinkFunction函数处理
 * 抽取方法
 * 抽取基类---模板方法设计模式
 * 执行流程（以修改了品牌维度表中的一条数据为例）
 * 当程序启动的时候，会将配置表中的配置信息加载到configMap以及广播状态中
 * 修改品牌维度
 * binlog会将修改操作记录下来
 * maxwell会从binlog中获取修改的信息，并封装为json格式字符串发送到kafka的topic_db主题中
 * DimApp应用程序会从topic_db主题中读取数据并对其进行处理
 * 根据当前处理的数据的表名判断是否为维度
 * 如果是维度的话，将维度数据传递到下游
 * 将维度数据同步到HBase中
 *
 * @author wenzhilong
 */
public class DimApp {
    public static void main(String[] args) throws Exception {

//        System.setProperty("HADOOP_USER_NAME", "root");
//
//        Configuration configuration = new Configuration();
//        configuration.setString("rest.port", "9099");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
//
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//
//        // start a checkpoint every 1000 ms
//        env.enableCheckpointing(5000);
//
//        // advanced options:
//
//        // set mode to exactly-once (this is the default)
//        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//
//        // make sure 500 ms of progress happen between checkpoints
//        checkpointConfig.setMinPauseBetweenCheckpoints(500);
//
//        // checkpoints have to complete within one minute, or are discarded
//        checkpointConfig.setCheckpointTimeout(60000);
//
//        // only two consecutive checkpoint failures are tolerated
//        checkpointConfig.setTolerableCheckpointFailureNumber(3);
//
//        // allow only one checkpoint to be in progress at the same time
//        checkpointConfig.setMaxConcurrentCheckpoints(1);
//
//        // enable externalized checkpoints which are retained
//        // after job cancellation
//        checkpointConfig.setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
//
//        // enables the unaligned checkpoints
//        checkpointConfig.enableUnalignedCheckpoints();
//
////         sets the checkpoint storage where checkpoint snapshots will be written
//        Configuration config = new Configuration();
//        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
//        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://hadoop102:8020/rt/checkpoint/DimApp");
//        env.configure(config);

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);

        System.setProperty("HADOOP_USER_NAME", "root");

        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        SingleOutputStreamOperator<JSONObject> dbLogStream = env
                .fromSource(
                        FlinkKafkaUtil.getConsumer(parameterTool, "ODS_BASE_DB"),
                        WatermarkStrategy.noWatermarks(),
                        "ODS_BASE_DB"
                )
                .setParallelism(2)
                .flatMap(new MaxwellLogEtl())
                .setParallelism(2);
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("root123")
                .databaseList("rt_warehouse_conf_db")
                .tableList("rt_warehouse_conf_db.table_process_dim")
                .deserializer(new JsonDebeziumDeserializationSchema())
                // initial: 扫描一次全表，做一次快照，然后增量
                .startupOptions(StartupOptions.initial())
                .build();

        SingleOutputStreamOperator<TableProcessDim> dimSourceStream = env
                .fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "table_process_dim"
                )
                .uid("table_process_dim")
                .setParallelism(1)
                .flatMap(new DimBinlogEtlAndBuilderHBaseTableFun())
                .setParallelism(1);

        // 配置广播
        MapStateDescriptor<String, TableProcessDim> dimBroadCastStateDescriptor =
                new MapStateDescriptor<String, TableProcessDim>("dimBroadCastStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> dimBroadcastStream = dimSourceStream.broadcast(dimBroadCastStateDescriptor);

        // 与业务流关联
        dbLogStream.connect(dimBroadcastStream)
                .process(new TableProcessFunction())
                .setParallelism(4)
                .addSink(new HBaseSinkFunction());
//                .print();

        env.execute();
    }
}
