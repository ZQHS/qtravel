package com.qf.bigdata.realtime.util.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Random;

/**
 * kafka自定义分区
 */
public class KafkaPartitionKeyUtil implements Partitioner {

    private Random random = new Random();

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }



    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);

        int position = 0;
        if(null == value){
            position = numPartitions-1;
        }else{
            String partitionInfo = key.toString();
            Integer num = Math.abs(partitionInfo.hashCode());
            //Long num = Long.valueOf(partitionInfo);
            Integer pos = num % numPartitions;
            position = pos.intValue();
            System.out.println("data partitions is " + position + ",num=" + num + ",numPartitions=" + numPartitions);
        }

        return position;
    }

    public static void main(String[] args) throws  Exception{

        

    }
}
