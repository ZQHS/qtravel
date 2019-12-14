package com.qf.bigdata.realtime.util;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class QkafkaSerializationSchema2 implements KafkaSerializationSchema<String> {

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
        return null;
    }
}
