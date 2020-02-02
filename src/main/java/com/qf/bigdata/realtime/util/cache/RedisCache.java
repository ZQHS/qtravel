package com.qf.bigdata.realtime.util.cache;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;

/**
 * redis客户端工具类
 */
public class RedisCache implements Serializable {

    private static JedisPool pool = null;
    private static Jedis executor = null;

    private static final int maxIdle = 10;
    private static final int minIdle = 2;
    private static final int maxTotal = 20;

    private GenericObjectPoolConfig createConfig(){
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(maxIdle);
        config.setMaxTotal(maxTotal);
        config.setMinIdle(minIdle);
        return config;
    }

    public JedisPool connectRedisPool(String ip){
        if(null == pool){
            GenericObjectPoolConfig config = createConfig();
            pool = new JedisPool(config, ip);
        }
        return pool;
    }

    public Jedis connectRedis(String ip, int port, String auth){
        if(null == executor){
            executor = new Jedis(ip, port);
            executor.auth(auth);
        }
        return executor;
    }

    public static void main(String[] args) {

        String ip = "node243";
        int port = 6379;
        String auth = "qfqf";

        RedisCache cache = new RedisCache();
        JedisPool pool = cache.connectRedisPool(ip);
        Jedis jedis = pool.getResource();
        jedis.auth(auth);

        //查询数据
//        String singleValue = CommonUtil.getRadomIP();
//        String key = "testnum";
//        jedis.set(key, singleValue);
//        String redisValue = jedis.get(key);
//        System.out.println("redisValue=" + redisValue);

        //复杂数据类型
//        String mapKey = "qf_nshop.t_test".replaceAll("\\.","_");
//
//        List<String> randomKeys = CommonUtil.getRangeNumber(1, 100, 1);
//        Map<String,String> datas = new HashMap<String,String>();
//        for(int i=1; i<=10; i++){
//            String randomKey = CommonUtil.getRandomElementRange(randomKeys);
//            String randomValue = CommonUtil.getRadomIP();
//            datas.put(randomKey, randomValue);
//            //byte[] datas = jedis.get(key.getBytes());
//            //descDatas = (Map<String,String>)ObjectTranscoder.deserialize(datas);
//        }
//        jedis.hmset(mapKey, datas);
//
//
//        Map<String,String> redisData = jedis.hgetAll(mapKey);
//        for(Map.Entry<String,String> entry : redisData.entrySet()){
//            String k = entry.getKey();
//            String v = entry.getValue();
//            System.out.println("redis.entry =" + k + " , " + v);
//        }


        //========================================
        String pubTable = "travel.dim_pub1";
        String pubID = "210480222|1a1bd08f";

        String pubV = jedis.hget(pubTable, pubID);
        System.out.println("redis.pub =" + pubID + " , pubValue=" + pubV);

//        Map<String,String> redisPubData = jedis.hgetAll(pubTable);
//        String pubValue = redisPubData.get(pubID);
//        System.out.println("redis.pub =" + pubID + " , pubValue=" + pubValue);



    }


}
