package com.at.rt.data.warehouse.utils;

import com.at.rt.data.warehouse.constant.FlinkConfConstant;
import com.at.rt.data.warehouse.constant.RTWarehouseConstant;
import com.at.rt.data.warehouse.exception.RTWarehouseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

/**
 * @author wenzhilong
 */
public class FlinkKafkaUtil {

    public static <T> KafkaSource<T> getConsumer(String bootstrapServers, String topic, String groupId, OffsetsInitializer offsetsInitializer, KafkaDeserializationSchema<T> schema) {
        return KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(KafkaRecordDeserializationSchema.of(schema))
                .build();
    }

    public static KafkaSource<String> getConsumer(String bootstrapServers, String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();
    }

    public static KafkaSource<String> getConsumer(String topic, String groupId) {
        return getConsumer(RTWarehouseConstant.KAFKA_BROKERS, topic, groupId);
    }

    public static KafkaSource<String> getConsumer(ParameterTool parameterTool, String prefix) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(parameterTool.get(String.format("kafka.consumer.%s.broker", prefix)))
                .setTopics(parameterTool.get(String.format("kafka.consumer.%s.topic", prefix)))
                .setGroupId(parameterTool.get(String.format("kafka.consumer.%s.group-id", prefix)))
                .setStartingOffsets(getOffsets(parameterTool, prefix))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();
    }

    public static <T> KafkaSink<T> getProduct(ParameterTool parameterTool, String prefix, KafkaRecordSerializationSchema<T> schema) {

        KafkaSinkBuilder<T> kafkaSinkBuilder = KafkaSink.<T>builder()
                .setBootstrapServers(parameterTool.get(String.format("kafka.product.%s.broker", prefix)))
                .setRecordSerializer(schema);

        setProductGuarantee(kafkaSinkBuilder, parameterTool, prefix);

        kafkaSinkBuilder.setKafkaProducerConfig(getProductProps(parameterTool, prefix));

        return kafkaSinkBuilder.build();
    }

    public static KafkaSink<String> getProduct(ParameterTool parameterTool, String prefix) {

//        KafkaSinkBuilder<String> kafkaSinkBuilder = KafkaSink.<String>builder()
//                .setBootstrapServers(parameterTool.get(String.format("kafka.product.%s.broker", prefix)))
//                .setRecordSerializer(
//                        KafkaRecordSerializationSchema.builder()
//                                .setTopic(parameterTool.get(String.format("kafka.product.%s.topic", prefix)))
//                                .setValueSerializationSchema(new SimpleStringSchema())
//                                .build()
//                );
//
//        setProductGuarantee(kafkaSinkBuilder, parameterTool, prefix);
//
//        kafkaSinkBuilder.setKafkaProducerConfig(getProductProps(parameterTool, prefix));
//
//        return kafkaSinkBuilder.build();

        KafkaRecordSerializationSchema<String> schema = KafkaRecordSerializationSchema.builder()
                .setTopic(parameterTool.get(String.format("kafka.product.%s.topic", prefix)))
                .setValueSerializationSchema(new SimpleStringSchema())
                .build();

        return getProduct(parameterTool, prefix, schema);
    }

    private static Properties getProductProps(ParameterTool parameterTool, String prefix) {
        Properties props = new Properties();

        return props;
    }

    private static <T> void setProductGuarantee(KafkaSinkBuilder<T> kafkaSinkBuilder, ParameterTool parameterTool, String prefix) {

        String guarantee = parameterTool.get(String.format("kafka.product.%s.guarantee", prefix));

        if (StringUtils.isBlank(guarantee)) {
            return;
        }

        if (parameterTool.getBoolean("isLocal") || "at-least-once".equals(guarantee)) {
            kafkaSinkBuilder.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
        } else if ("exactly-once".equals(guarantee)) {

            if (!parameterTool.getBoolean(FlinkConfConstant.CHECKPOINT_ENABLE)) {
                throw new RTWarehouseException("sink set exactly-once to have checkpoint.");
            }

            kafkaSinkBuilder
                    //当前配置决定是否开启事务，保证写到kafka数据的精准一次
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    //设置事务Id的前缀
                    .setTransactionalIdPrefix(
                            parameterTool.get(
                                    String.format("kafka.product.%s.tid-prefix", prefix, UUID.randomUUID().toString())
                            )
                    )
                    //设置事务的超时时间   检查点超时时间 <     事务的超时时间 <= 事务最大超时时间
                    .setProperty(
                            ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                            parameterTool.get(String.format("kafka.product.%s.transaction-timeout", prefix))
                    );
        } else {
            kafkaSinkBuilder.setDeliveryGuarantee(DeliveryGuarantee.NONE);
        }
    }

    private static OffsetsInitializer getOffsets(ParameterTool parameterTool, String prefix) {

        String offset = parameterTool.get(String.format("kafka.consumer.%s.offset", prefix));

        // latest earliest  timestamp group
        switch (offset) {
            case "earliest":
                return OffsetsInitializer.earliest();
            case "timestamp":
                return OffsetsInitializer.timestamp(
                        parameterTool.getLong(String.format("kafka.consumer.%s.timestamp-millis", prefix))
                );
            case "latest":
                return OffsetsInitializer.latest();
            default:
                return OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST);
        }
    }
}
