package com.qf.bigdata.realtime.util.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.qf.bigdata.realtime.util.CommonUtil;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GuavaCache implements Serializable {


    public static com.google.common.cache.CacheLoader<String, String> createCacheLoader() {
        return new com.google.common.cache.CacheLoader<String, String>() {
            @Override
            public String load(String key) throws Exception {
                String value = "value"+key;
                return value;
            }
        };
    }


    public static void main(String[] args) {
        Cache<String, String> cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterAccess(30L, TimeUnit.MILLISECONDS)
                .build();

        List<String> randomKeys = CommonUtil.getRangeNumber(1, 100, 1);
        for(int i=1; i<=50; i++) {
            String randomKey = CommonUtil.getRandomElementRange(randomKeys);
            cache.put(String.valueOf(i), randomKey);
        }

        for(int i=1;i<100;i++){
            String randomKey = CommonUtil.getRandomElementRange(randomKeys);
            String value = cache.getIfPresent(randomKey);
            System.out.println("["+i+"] randomKey" + randomKey + "," + value);
        }

                //.build(createCacheLoader());
    }

}
