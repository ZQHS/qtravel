package com.qianfeng.bigdata.realtime.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;

/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date redis连接工具
*@Description
**/
public class RedisCache implements Serializable {

    //定义pool和单独的jedis
    private static JedisPool pool = null;
    private static Jedis jedis = null;

    //连接池初始化参数
    private static final int port = 6379;
    private static final int timeout = 10 * 1000;
    private  static final int maxIdle = 10;
    private static final int minIdle = 2;
    private static final int maxTotal = 20;

    //將初始化參數放到配置對象中
    private GenericObjectPoolConfig createConfig(){
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        //添加初始值
        config.setMaxIdle(maxIdle);
        config.setMaxTotal(maxTotal);
        config.setMinIdle(minIdle);
        //返回
        return config;
    }

    //獲取jedis的連接池
    public JedisPool connectRedisPool(String ip){
        if(pool == null){
            GenericObjectPoolConfig config = createConfig();
            pool = new JedisPool(config, ip, port, timeout);
        }
        return pool;
    }

    //獲取jedis的連接
    public Jedis connectJedis(String ip,int port,String auth){
        if(jedis == null){
            jedis = new Jedis(ip,port);
            //設置密碼
            jedis.auth(auth);
        }
        return jedis;
    }

    //測試單個連接和連接池
    public static void main(String[] args) {
        RedisCache redisCache = new RedisCache();
        Jedis jedis = redisCache.connectJedis("hadoop01", port, "root");
        //測試連接
        System.out.println(jedis.ping());

        JedisPool pool = redisCache.connectRedisPool("hadoop01");
        Jedis jedis1 = pool.getResource();
        jedis1.auth("root");
        System.out.println(jedis1.ping());

        //存数据
        jedis.set("java_redis1","nice flink");
    }
}
