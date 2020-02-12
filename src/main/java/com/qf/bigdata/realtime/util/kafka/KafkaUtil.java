package com.qf.bigdata.realtime.util.kafka;

import com.qf.bigdata.realtime.constant.TravelConstant;
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant;
import com.qf.bigdata.realtime.util.zookeeper.ZKClientUtil;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by hp on 2017/3/22.
 */
public class KafkaUtil {

    public static final String KAFKA_PRODUCER_CONFIG = "kafka/json/kafka-producer.properties";
    public static final String KAFKA_CONSUMER_CONFIG = "kafka/json/kafka-consumer.properties";

    //默认分区数量
    public static final Integer KAFKA_PARTITIONS_DEFCOUNT = 5;
    public static final Integer KAFKA_REPLICATIONS_DEFCOUNT = 3;


    /**
     * 读取配置文件
     * @return
     */
    public static Properties readKafkaProps(String path) throws IOException{
        Properties props = new Properties();
        props.load(KafkaUtil.class.getClassLoader().getResourceAsStream(path));
        return props;
    }


    /**
     * 读取配置文件 覆盖属性值
     * @param path
     * @param config
     * @return
     */
    public static Properties readKafkaProps(String path, Map<String, String> config){
        Properties props = null;
        try{
            props = new Properties();
            props.load(KafkaUtil.class.getClassLoader().getResourceAsStream(path));
            if(null != config){
                for(Map.Entry<String, String> entry : config.entrySet()){
                    props.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }catch(IOException ioe){
            ioe.printStackTrace();
        }
        return props;
    }


    /**
     * 连接zookeeper
     */
//    private ZkClient connZKClient() {
//        if(null == zkClient){
//            zkClient = new ZkClient(
//                    zkUrl,
//                    zkSessionTimeout,
//                    zkConnTimeout,
//                    ZKStringSerializer$.MODULE$);
//        }
//        return zkClient;
//    }
//
//    /**
//     * 关闭zk连接
//     */
    public void closeZkClient(){
        ZKClientUtil.closeZkClient();
    }
//
//
//    /**
//     * 创建zkUtils
//     * @return
//     */
    public ZkUtils createZkUtils() throws Exception{
        return ZKClientUtil.createZkUtils4Kafka();
    }

    /**
     * 关闭zk连接
     */
    public void closeZkUtils(){
        ZKClientUtil.closeZkUtils();
    }


    /**
     * 创建topic
     * @param topic
     * @param partitions
     * @param replication
     */
    public boolean  createTopic(String topic, Integer partitions, Integer replication){
        Validate.notEmpty(topic, "topic must be not empty");
        Validate.notNull(partitions, "partitions must be not empty");
        Validate.notNull(replication, "replication must be not empty");

        boolean result = true;
        try{
            ZkUtils zkUtils = createZkUtils();
            if(null != zkUtils){
                boolean exist = AdminUtils.topicExists(zkUtils, topic);
                if(!exist){
                    AdminUtils.createTopic(zkUtils, topic, partitions, replication, new Properties(), kafka.admin.RackAwareMode.Enforced$.MODULE$);
                }
            }
        }catch(Exception e){
            result = false;
            e.printStackTrace();
        }finally{
            //closeZkUtils();
            //closeZkClient();
        }
        return result;
    }


    public boolean  deteleGroup(String group){
        Validate.notNull(group, "group must be not empty");

        boolean result = true;
        try{
            ZkUtils zkUtils = createZkUtils();
            if(null != zkUtils){
                result = AdminUtils.deleteConsumerGroupInZK(zkUtils,group);
            }
        }catch(Exception e){
            result = false;
            e.printStackTrace();
        }finally{
            //closeZkUtils();
            //closeZkClient();
        }
        return result;
    }


    public boolean  deleteTopic(String topic){
        Validate.notEmpty(topic, "topic must be not empty");

        boolean result = true;
        try{
            ZkUtils zkUtils = createZkUtils();
            if(null != zkUtils){
                boolean exist = AdminUtils.topicExists(zkUtils, topic);
                if(exist){
                    AdminUtils.deleteTopic(zkUtils, topic);
                }
            }
        }catch(Exception e){
            result = false;
            e.printStackTrace();
        }finally{
            //closeZkUtils();
            //closeZkClient();
        }
        return result;
    }


    /**
     * topic是否存在
     * @param topic
     * @return
     */
    public boolean  existTopic(String topic){
        Validate.notEmpty(topic, "topic must be not empty");

        boolean result = false;
        try{
            ZkUtils zkUtils = createZkUtils();
            if(null != zkUtils){
                result = AdminUtils.topicExists(zkUtils, topic);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            //closeZkUtils();
            //closeZkClient();
        }
        return result;
    }


    /**
     * 查看topic
     */
    public List<String> listTopic(){
        List<String> topics = new ArrayList<String>();
        try{
            ZkUtils zkUtils = createZkUtils();
            if(null != zkUtils){
                topics = scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            //closeZkUtils();
            //closeZkClient();
        }

        return topics;
    }



    //-----------------------------------------------

    /**
     * 创建生产者
     * @return
     */
    public static KafkaProducer<String, String> createProducer(String path) throws IOException {
        Validate.notEmpty(path, "path must be not empty");
        KafkaProducer<String, String>  producer = null;
        Properties props = readKafkaProps(path);
        if(null != props){
            producer = new KafkaProducer<String, String>(props);
        }
        return producer;
    }

    public static KafkaProducer<String, byte[]> createProducer4Bytes(String path) throws IOException{
        Validate.notEmpty(path, "path must be not empty");

        KafkaProducer<String, byte[]>  producer = null;
        Properties props = readKafkaProps(path);
        if(null != props){
            producer = new KafkaProducer<String, byte[]>(props);
        }
        return producer;
    }

    public static void closeProducer(Producer producer){
        Validate.notNull(producer, "producer is Null");

        producer.close();
    }

    public static KafkaConsumer<String,String> createConsumer(String path) throws IOException{
        Validate.notEmpty(path, "path must be not empty");

        Properties props = KafkaUtil.readKafkaProps(path);
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        return consumer;
    }

    public static KafkaConsumer<String,byte[]> createConsumer4Bytes(String path) throws IOException{
        Validate.notEmpty(path, "path must be not empty");

        Properties props = KafkaUtil.readKafkaProps(path);
        KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
        return consumer;
    }

    public static KafkaConsumer<String,byte[]> createConsumer4Obj(String path) throws IOException{
        Validate.notEmpty(path, "path must be not empty");

        Properties props = KafkaUtil.readKafkaProps(path);
        KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
        return consumer;
    }

    public static void main(String[] args){

        String zkServers = "node242:2181,node243:2181,node244:2181/kafka";
        int zkSessionTimeout = 5000;
        int zkConnTimeout = 5000;

        int partitions = KAFKA_PARTITIONS_DEFCOUNT;
        int replication = KAFKA_REPLICATIONS_DEFCOUNT;

        KafkaUtil kafkaUtil = new KafkaUtil();

        //是否存在
        //kafkaUtil.existTopic(topic);

        //创建
        String topic = "topic_log_action_view"; //topic_orders_dw topic_orders_dw_wide
        List<String> topicList = new ArrayList<String>();
        topicList.add(topic);
        for(String t : topicList){
            //kafkaUtil.createTopic(t, partitions, replication);
        }


        String group = "test";
        //kafkaUtil.deteleGroup(group);

        //删除
        //List<String> topicList = new ArrayList<String>();



        //列表
        List<String> topics = kafkaUtil.listTopic();
        for(String tpc : topics){
            System.out.println(tpc);
        }

    }

}
