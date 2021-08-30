package com.qianfeng.bigdata.realtime.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date kafka分区工具类
*@Description
**/
public class KafkaPartitionKeyUtil implements Partitioner {
    public void close() {
    }

    public void configure(Map<String, ?> configs) {
    }

    //使用其来分区，返回分区编号
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取kafka的主题的分区
        int numPartitions = cluster.partitionCountForTopic(topic);

        int position = 0;
        //如果value为空，则默认将其放到主题分区数-1编号的分区中即可
        if(null == value){
            position = numPartitions-1;
        }else{
            //value不等于null，，使用key的hashcode()%numPartitions=分区编号
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
        System.out.println("aaaaaaaa");
    }
}
