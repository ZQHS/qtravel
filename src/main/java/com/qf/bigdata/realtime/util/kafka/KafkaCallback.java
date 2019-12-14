package com.qf.bigdata.realtime.util.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * kafka生产者回调
 */
public class KafkaCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        int partition = metadata.partition();
        String topic = metadata.topic();
        long offset = metadata.offset();
        System.out.println("topic=" + topic + ",partition=" + partition + ",offset=" + offset);
    }

}
