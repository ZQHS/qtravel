package com.qf.bigdata.realtime.util.kafka.producer;


import com.qf.bigdata.realtime.util.kafka.KafkaUtil;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * 消息生产者工具类
 */
public class KafkaProducerUtil {

    private final static Logger log = LoggerFactory.getLogger(KafkaProducerUtil.class);

    /**
     * json数据
     * @param path
     * @param topic
     * @param datas
     */
    public static void sendMsg(String path, String topic, Map<String,String> datas) throws Exception{
        Validate.notEmpty(path, "kafka config path is not empty");
        Validate.notEmpty(topic, "topic is not empty");
        Validate.notNull(datas, "datas is not empty");

        KafkaProducer producer = KafkaUtil.createProducer(path);
        if(null != producer){
            List<String> lines = new ArrayList<String>();
            for(Map.Entry<String, String> entry : datas.entrySet()){
                String key = entry.getKey();
                String value = entry.getValue();
                log.info("Producer.key=" + key + ",value=" + value);

                producer.send(new ProducerRecord<String, String>(topic, key, value));
                producer.flush();
            }
            producer.close();
        }

    }




    /**
     * kryo序列化
     * @param path
     * @param topic
     * @param datas
     */
    public static void sendMsg4Kryo(String path, String topic, Map<String,Serializable> datas){
        Validate.notEmpty(path, "kafka config path is not empty");
        Validate.notEmpty(topic, "topic is not empty");
        Validate.notNull(datas, "datas is not empty");
        try{
            Properties props = KafkaUtil.readKafkaProps(path);
            KafkaProducer<String, Serializable>  producer = new KafkaProducer<String, Serializable>(props);
            if(null != producer){
                for(Map.Entry<String, Serializable> entry : datas.entrySet()){
                    String key = entry.getKey();
                    Serializable value = entry.getValue();
                    log.info("Producer.key=" + key + ",value=" + value);
                    producer.send(new ProducerRecord<String, Serializable>(topic, key, value));
                    producer.flush();
                }
                producer.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }



    //==========================================================================
    public static void main(String[] args) throws  Exception {





    }
}
