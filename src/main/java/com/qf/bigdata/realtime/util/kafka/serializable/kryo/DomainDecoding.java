package com.qf.bigdata.realtime.util.kafka.serializable.kryo;



import com.qf.bigdata.realtime.constant.CommonConstant;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Administrator on 2017-6-15.
 */
public class DomainDecoding implements org.apache.kafka.common.serialization.Deserializer<Serializable> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Serializable deserialize(String topic, byte[] data) {
        Serializable obj = null;
        try{
            if(null != data){
                if(CommonConstant.TOPIC_TEST.equalsIgnoreCase(topic)){
                    //obj = KryoUtil.deserializationObject(data, ReleaseBidding.class);
                }

            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return obj;
    }

    @Override
    public void close() {

    }
}
