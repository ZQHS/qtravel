package com.qianfeng.bigdata.realtime.util;

import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 获取属性配置文件
*@Description
**/
public class PropertyUtil implements Serializable {
    //日誌打印對象
    private static Logger log = LoggerFactory.getLogger(PropertyUtil.class);

    /**
     * 读取资源文件
     * @param proPath
     * @return
     */
    public static Properties readProperties(String proPath){
        //定义变量
        Properties properties = null;
        InputStream is = null;
        try{
            //类加载器加载指定property文件
            is = PropertyUtil.class.getClassLoader().getResourceAsStream(proPath);
            properties = new Properties();
            properties.load(is);
        }catch(IOException ioe){
            log.error("loadProperties:" + ioe.getMessage());
        }finally {
            try{
                if(null != is){
                    is.close();
                }}catch (Exception e){
                e.printStackTrace();
            }
        }
        return properties;
    }

    //测试
    public static void main(String[] args) {
        System.out.println(PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL()).getProperty("bootstrap.servers"));
    }
}
