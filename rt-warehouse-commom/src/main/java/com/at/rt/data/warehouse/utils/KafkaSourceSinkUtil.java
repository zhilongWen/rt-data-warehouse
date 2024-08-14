//package com.at.rt.data.warehouse.utils;
//
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//
//public class KafkaSourceSinkUtil {
//
//    public void getConsumer(S){
//
//        KafkaSource.<String>builder()
//                .setBootstrapServers(Constant.KAFKA_BROKERS)
//                .setTopics(topicName)
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setGroupId(groupId)
//                .setValueOnlyDeserializer(
////                        new SimpleStringSchema()
//                        new DeserializationSchema<String>() {
//                            @Override
//                            public String deserialize(byte[] message) throws IOException {
//                                if(message != null && message.length != 0) {
//                                    return new String(message, StandardCharsets.UTF_8);
//                                }
//
//                                return "";
//                            }
//
//                            @Override
//                            public boolean isEndOfStream(String s) {
//                                return false;
//                            }
//
//                            @Override
//                            public TypeInformation<String> getProducedType() {
//                                return BasicTypeInfo.STRING_TYPE_INFO;
//                            }
//                        }
//                )
//                .build();
//
//    }
//
//}
