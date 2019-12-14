package com.qf.bigdata.realtime.util.json;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.type.JavaType;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class JsonMapperUtil implements Serializable {

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        //将对象的所有字段全部列入
        mapper.setSerializationInclusion(JsonSerialize.Inclusion.ALWAYS);

        //取消默认转换timestamps形式,false使用日期格式转换，true不使用日期转换，结果是时间的数值157113793535
        mapper.configure(SerializationConfig.Feature.WRITE_DATE_KEYS_AS_TIMESTAMPS,false); //默认值true

        //忽略空Bean转json的错误
        mapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS,false);

        //所有的日期格式统一样式： yyyy-MM-dd HH:mm:ss
        mapper.setDateFormat(new SimpleDateFormat("yyyyMMddHHmmss"));

        //忽略 在json字符串中存在，但是在对象中不存在对应属性的情况，防止错误。
        // 例如json数据中多出字段，而对象中没有此字段。如果设置true，抛出异常，因为字段不对应；false则忽略多出的字段，默认值为null，将其他字段反序列化成功
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_NULL_FOR_PRIMITIVES,false);

    }


    public static <T> T readValue(String value, Class<T> valueType) throws Exception{
        return mapper.readValue(value, valueType);
    }


    //将单个对象转换成json格式的字符串（没有格式化后的json）
    public static <T> String obj2String(T obj){
        if (obj == null){
            return null;
        }
        try {
            return obj instanceof String ? (String) obj:mapper.writeValueAsString(obj);
        } catch (IOException e) {
            return null;
        }
    }


    public static void main(String[] args) {
        Map<String,String> input = new HashMap<String,String>();
        input.put("1","a");
        input.put("2","b");

        String json = obj2String(input);
        System.out.println(json);
    }


}
