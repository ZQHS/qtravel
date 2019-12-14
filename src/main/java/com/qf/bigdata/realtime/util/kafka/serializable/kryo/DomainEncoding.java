package com.qf.bigdata.realtime.util.kafka.serializable.kryo;



import com.qf.bigdata.sharecar.util.kryo.KryoUtil;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Administrator on 2017-6-15.
 */
public class DomainEncoding implements org.apache.kafka.common.serialization.Serializer<Serializable> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Serializable data) {
        byte[] datas = null;
        try{
            datas = KryoUtil.serializationObject(data);
        }catch(Exception e){
            e.printStackTrace();
        }
        return datas;
    }

    @Override
    public void close() {

    }
}
