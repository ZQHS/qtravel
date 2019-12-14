package com.qf.bigdata.realtime.util.zookeeper;

import com.qf.bigdata.realtime.constant.CommonConstant;
import com.qf.bigdata.realtime.util.PropertyUtil;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.common.security.JaasUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

public class ZKClientUtil implements Serializable {

    public static final String ZK_PATH_SEPTAL = "/";

    private static ZkClient zkClient;
    private static ZkUtils zkUtils;

    private static String zkKafkaUrl;
    private static String zkUrl;
    private static Integer zkSessionTimeout = 60000;
    private static Integer zkConnTimeout = 10000;
    public static String zkBeeRoot;

    public static final  String zkConf = "zk/zk.properties";

    static{
        Properties zkProperty = PropertyUtil.readProperties(ZKClientUtil.zkConf);

        zkKafkaUrl = zkProperty.getProperty(CommonConstant.ZK_CONNECT_KAFKA);
        zkUrl = zkProperty.getProperty(CommonConstant.ZK_CONNECT);
        zkBeeRoot = zkProperty.getProperty(CommonConstant.ZK_BEE_ROOT);
        zkSessionTimeout = Integer.valueOf(zkProperty.getProperty(CommonConstant.ZK_SESSION_TIMEOUT)) * 1000;
        zkConnTimeout = Integer.valueOf(zkProperty.getProperty(CommonConstant.ZK_CONN_TIMEOUT)) * 1000;

        Validate.notEmpty(zkUrl, "zkUrl must a not empty");
        Validate.notNull(zkSessionTimeout, "zkSessionTimeout must a not empty");
        Validate.notNull(zkConnTimeout, "zkConnTimeout must a not empty");
    }

    /**
     * 连接zookeeper
     */
    public static synchronized ZkClient connZKClient() throws Exception{
        if(null == zkClient){
            zkClient = new ZkClient(
                    zkUrl,
                    zkSessionTimeout,
                    zkConnTimeout,
                    ZKStringSerializer$.MODULE$);
        }
        return zkClient;
    }


    public static synchronized ZkClient connZKClient4Kafka() throws Exception{
        if(null == zkClient){
            zkClient = new ZkClient(
                    zkKafkaUrl,
                    zkSessionTimeout,
                    zkConnTimeout,
                    ZKStringSerializer$.MODULE$);
        }
        return zkClient;
    }


    /**
     * 关闭zk连接
     */
    public static synchronized void closeZkClient(){
        if(null != zkClient){
            zkClient.close();
        }
    }


    /**
     * 创建zkUtils
     * @return
     */
    public static synchronized ZkUtils createZkUtils() throws Exception{
        ZkClient zkClient = connZKClient();
        if(null == zkUtils){
            boolean isSecureCluster = JaasUtils.isZkSecurityEnabled();
            System.out.println("isSecureCluster=" + isSecureCluster);
            zkUtils = new ZkUtils(zkClient,
                    new ZkConnection(zkUrl),
                    isSecureCluster);
        }

        return zkUtils;
    }

    public static synchronized ZkUtils createZkUtils4Kafka() throws Exception{
        ZkClient zkClient = connZKClient4Kafka();
        if(null == zkUtils){
            boolean isSecureCluster = JaasUtils.isZkSecurityEnabled();
            //System.out.println("isSecureCluster=" + isSecureCluster);
            zkUtils = new ZkUtils(zkClient,
                    new ZkConnection(zkKafkaUrl),
                    isSecureCluster);
        }

        return zkUtils;
    }



    /**
     * 关闭zk连接
     */
    public static synchronized void closeZkUtils(){
        if(null != zkUtils){
            zkUtils.close();
        }
    }


    /**
     * 判断zk节点是否存在
     * @param path
     * @return
     * @throws Exception
     */
    public static boolean exist(String path) throws Exception{
        ZkClient zkClient = connZKClient();
        if(null != zkClient){
            return zkClient.exists(path);
        }
        return false;
    }


    /**
     * 创建zk永久节点
     * @param path
     * @param createParent
     * @throws Exception
     */
    public static void createPersistent(String path, boolean createParent) throws Exception{
        ZkClient zkClient = connZKClient();
        if(null != zkClient){
            if(!exist(path)){
                zkClient.createPersistent(path, createParent);
            }
        }
    }

    /**
     * 创建zk临时节点
     * @param path
     * @throws Exception
     */
    public static void createEphemeral(String path) throws Exception{
        ZkClient zkClient = connZKClient();
        if(null != zkClient){
            if(!exist(path)){
                zkClient.createEphemeral(path);
            }
        }
    }


    /**
     * 删除zk节点数据
     * @param path
     * @return
     * @throws Exception
     */
    public static boolean deleteLeaf(String path) throws Exception{
        ZkClient zkClient = connZKClient();
        if(null != zkClient){
            if(!exist(path)){
                return false;
            }else{
                return zkClient.delete(path);
            }
        }
        return false;
    }

    public static boolean deleteRecursive(String path) throws Exception{
        ZkClient zkClient = connZKClient();
        if(null != zkClient){
            if(!exist(path)){
                return false;
            }else{
                return zkClient.deleteRecursive(path);
            }
        }
        return false;
    }


    /**
     * 判断zk节点是否存在
     * @param path
     * @return
     * @throws Exception
     */
    public static List<String> getChildren(String path) throws Exception{
        ZkClient zkClient = connZKClient();
        if(null != zkClient){
            return zkClient.getChildren(path);
        }
        return null;
    }

    public static int getChildrenCount(String path) throws Exception{
        ZkClient zkClient = connZKClient();
        if(null != zkClient){
            if(exist(path)){
                return zkClient.countChildren(path);
            }
        }
        return 0;
    }


    /**
     * 读取zk节点数据
     * @param path
     * @return
     * @throws Exception
     */
    public static Object read(String path) throws Exception{
        ZkClient zkClient = connZKClient();
        if(null != zkClient){
            if(exist(path)){
                return zkClient.readData(path);
            }
        }
        return null;
    }


    /**
     * 写入zk节点数据
     * @param path
     * @param data
     * @throws Exception
     */
    public static void write(String path, Object data) throws Exception{
        ZkClient zkClient = connZKClient();
        if(null != zkClient){
            if(!exist(path)){
                createPersistent(path, true);
            }
            zkClient.writeData(path, data);
        }
    }



    public static void main(String[] args) throws  Exception{

        //ZkUtils zkUtils = createZkUtils();

        ZkClient zkClient = connZKClient();

        String path = "/dw/zk/";
        zkClient.createPersistent(path, true);

//        List<String> datas = getChildren("/bee/offset/bike");
//        for(String data : datas){
//            System.out.println(data);
//        }

        //System.out.println(getChildrenCount("/bee/offset/battery/"));

        //删除
        //String path1 = "/mybee";
        //deleteRecursive(path1);

        //读数据
//        String path = "/bee/topic/battery/12";
//        Object datas = read(path);
//        System.out.println(datas);



    }

}
