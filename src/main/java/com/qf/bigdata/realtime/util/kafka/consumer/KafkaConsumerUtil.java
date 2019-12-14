package com.qf.bigdata.realtime.util.kafka.consumer;

import com.qf.bigdata.realtime.constant.CommonConstant;
import com.qf.bigdata.realtime.util.CommonUtil;
import com.qf.bigdata.realtime.util.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

/**
 * Created by hp on 2017/5/19.
 */
public class KafkaConsumerUtil {

    /**
     * 消费(text)
     */
    public static void consume(String path, String topic ,long pollTimeout) {
        Consumer<String, String> consumer = null;
        try{
            consumer = KafkaUtil.createConsumer(path);
            consumer.subscribe(Arrays.asList(topic));
            boolean isRunning = true;
            while(isRunning) {
                ConsumerRecords<String,String> consumerRecords = consumer.poll(pollTimeout);
                for(ConsumerRecord<String, String> record : consumerRecords){
                    int partitionIdx = record.partition();
                    String topicName = record.topic();
                    String key = record.key();
                    String value = record.value();

                    System.out.println("Thread,partitionIdx="+ partitionIdx+",key="+ key + ",value=" + value);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(null != consumer){
                consumer.close();
            }
        }

    }


    /**
     * 消费(kryo格式)
     * @param path
     * @param topic
     * @param pollTimeout
     */
    public static void consume4Kryo(String path, String topic ,long pollTimeout) {
        KafkaConsumer<String,Serializable> consumer = null;
        try{

            Properties props = KafkaUtil.readKafkaProps(path);
            consumer = new KafkaConsumer<String,Serializable>(props);

            consumer.subscribe(Arrays.asList(topic));
            boolean isRunning = true;
            while(isRunning) {
                ConsumerRecords<String,Serializable> consumerRecords = consumer.poll(pollTimeout);

                for(ConsumerRecord<String, Serializable> record : consumerRecords){
                    int partitionIdx = record.partition();
                    String topicName = record.topic();
                    String key = record.key();
                    Serializable values = record.value();

                    System.out.println("Thread,partitionIdx="+ partitionIdx+",key="+ key + ",value=" + values);
                }

            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(null != consumer){
                consumer.close();
            }
        }

    }


    public static void testKryo() throws Exception{
        String path = "kafka/kryo/kafka-consumer.properties";
        //String topic = BeeConstant.TOPIC_BATTERY;
        String topic = CommonConstant.TOPIC_TEST;
        String group = "TEST";
        long pollTimeout = 100l;


        //消费消息
        consume4Kryo(path, topic ,pollTimeout);
        System.out.println("kafka producer send =" + CommonUtil.formatDate4Def(new Date()));
    }


    public static void main(String[] args) throws Exception{

        String path = CommonConstant.KAFKA_CONSUMER_JSON_PATH;
        String topic = CommonConstant.TOPIC_TEST;
        long pollTimeout = 100l;
        consume(path, topic , pollTimeout);

    }

}
