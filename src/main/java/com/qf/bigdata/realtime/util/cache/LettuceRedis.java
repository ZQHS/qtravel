package com.qf.bigdata.realtime.util.cache;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.qf.bigdata.realtime.util.CommonUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * redis客户端框架Lettuce
 */
public class LettuceRedis implements Serializable {

    private static RedisClient client;

    public static RedisClient connectRedisLettuce(String ip, int port, String auth){
        client = RedisClient.create(RedisURI.create(ip, port));
        return client;
    }

    public static void connectRedis(String ip, int port, String auth){
        client = RedisClient.create(RedisURI.create(ip, port));
        StatefulRedisConnection<String,String> conn = client.connect();
        RedisCommands<String,String> command = conn.sync();
        command.auth(auth);
    }



    public static void main(String[] args) {

        String ip = "node243";
        int port = 6379;
        String auth = "qfqf";
        RedisClient client = connectRedisLettuce(ip, port, auth);
        StatefulRedisConnection<String,String> conn = client.connect();
        RedisCommands<String,String> command = conn.sync();
        command.auth(auth);

//        //查询数据
//        String singleValue = CommonUtil.getRadomIP();
//        String key = "testnum2";
//        command.set(key, singleValue);
//        String redisValue = command.get(key);
//        System.out.println("redisValue=" + redisValue);
//
//        //复杂数据类型
//        //String mapKey = "qf_nshop.t_test".replaceAll("\\.","_");
//
//        String mapKey = "testmap2";
//        List<String> randomKeys = CommonUtil.getRangeNumber(1, 100, 1);
//        Map<String,String> datas = new HashMap<String,String>();
//        for(int i=1; i<=10; i++){
//            String randomKey = CommonUtil.getRandomElementRange(randomKeys);
//            String randomValue = CommonUtil.getRadomIP();
//            datas.put(randomKey, randomValue);
//            //byte[] datas = jedis.get(key.getBytes());
//            //descDatas = (Map<String,String>)ObjectTranscoder.deserialize(datas);
//        }
//        command.hmset(mapKey, datas);
//
//
//        Map<String,String> redisData = command.hgetall(mapKey);
//        for(Map.Entry<String,String> entry : redisData.entrySet()){
//            String k = entry.getKey();
//            String v = entry.getValue();
//            System.out.println("redis.entry =" + k + " , " + v);
//        }

        //========================================
        String pubTable = "travel.dim_pub1";
        String pubID = "210480222|1a1bd08f";
        Map<String,String> redisPubData = command.hgetall(pubTable);
        String pubValue = redisPubData.get(pubID);
        System.out.println("redis.pub =" + pubID + " , pubValue=" + pubValue);







    }

}
