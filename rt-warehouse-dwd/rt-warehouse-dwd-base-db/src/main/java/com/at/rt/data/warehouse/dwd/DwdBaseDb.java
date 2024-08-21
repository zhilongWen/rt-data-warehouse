package com.at.rt.data.warehouse.dwd;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import com.at.rt.data.warehouse.bean.TableProcessConfDwd;
import com.at.rt.data.warehouse.utils.FlinkKafkaUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 处理逻辑比较简单的事实表动态分流处理
 *
 * @author wenzhilong
 */
public class DwdBaseDb {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecEnvConf.builderStreamEnv(args);
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        // 对流中的数据进行类型转换并进行简单的 ETL jsonStr->jsonObj
        SingleOutputStreamOperator<JsonNode> dbStream = env
                .fromSource(
                        FlinkKafkaUtil.getConsumer(parameterTool, "dwd_base_db_log_split"),
                        WatermarkStrategy.noWatermarks(),
                        "dwd_base_db_log_split"
                )
                .flatMap(new ParseDBLogFun());


        // 使用 FlinkCDC 读取配置表中的配置信息
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("root123")
                .databaseList("rt_warehouse_conf_db")
                .tableList("rt_warehouse_conf_db.table_process_dwd_conf")
                .deserializer(new JsonDebeziumDeserializationSchema())
                // initial: 扫描一次全表，做一次快照，然后增量
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();

        SingleOutputStreamOperator<TableProcessConfDwd> dimBinlogStream = env
                .fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "table_process_dwd_conf"
                )
                .uid("table_process_dwd_conf")
                .setParallelism(1)
                .flatMap(new ParseDimBinlogFun())
                .setParallelism(1);

        // 对配置流进行广播 ---broadcast
        MapStateDescriptor<String, TableProcessConfDwd> mapStateDescriptor = new MapStateDescriptor<>(
                "dimConfState", String.class, TableProcessConfDwd.class);
        BroadcastStream<TableProcessConfDwd> dimbroadcastStream = dimBinlogStream.broadcast(mapStateDescriptor);

        // 关联主流业务数据和广播流中的配置数据   --- connect
        dbStream.connect(dimbroadcastStream)
                // 对关联后的数据进行处理   --- process
                .process(new BaseDbTableProcessFunction(mapStateDescriptor))
                // 将处理逻辑比较简单的事实表数据写到kafka的不同主题中
                .sinkTo(
                        KafkaSink.<Tuple2<JsonNode, TableProcessConfDwd>>builder()
                                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JsonNode, TableProcessConfDwd>>() {
                                    @Nullable
                                    @Override
                                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JsonNode, TableProcessConfDwd> element, KafkaSinkContext context, Long timestamp) {
                                        return new ProducerRecord<byte[], byte[]>(element.f1.getSinkTable(), element.f0.toString().getBytes(StandardCharsets.UTF_8));
                                    }
                                })
                                .build()
                );

        env.execute();
    }
}
