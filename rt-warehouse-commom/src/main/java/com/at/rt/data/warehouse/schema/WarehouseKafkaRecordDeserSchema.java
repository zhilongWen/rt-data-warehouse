package com.at.rt.data.warehouse.schema;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * @author wenzhilong
 */
public class WarehouseKafkaRecordDeserSchema<V> implements KafkaDeserializationSchema<ConsumerRecord<String, V>> {

    private static final long serialVersionUID = 1L;

    private final ConsumerFunction<byte[], V> userFunction;

    public <F extends ConsumerFunction<byte[], V>> WarehouseKafkaRecordDeserSchema(F userFunction) {
        this.userFunction = userFunction;
    }

    @Override
    public TypeInformation<ConsumerRecord<String, V>> getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumerRecord<String, V>>() {
        });
    }

    @Override
    public boolean isEndOfStream(ConsumerRecord<String, V> nextElement) {
        return false;
    }

    @Override
    public ConsumerRecord<String, V> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

        byte[] keyBytes = Optional.ofNullable(record.key()).orElseGet(() -> new byte[]{});
        byte[] valueBytes = Optional.ofNullable(record.value()).orElseGet(() -> new byte[]{});

        String k = new String(keyBytes, StandardCharsets.UTF_8);
        V v = userFunction.apply(valueBytes);

        return new ConsumerRecord<>(
                record.topic(),
                record.partition(),
                record.offset(),
                System.currentTimeMillis(),
                record.timestampType(),
                record.serializedKeySize(),
                record.serializedValueSize(),
                k,
                v,
                record.headers(),
                record.leaderEpoch()

        );
    }
}
