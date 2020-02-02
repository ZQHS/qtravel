package com.qf.bigdata.realtime.util.format;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * parquet文件工具类
 */
public class QParquetHelper implements Serializable {

    private final static Logger log = LoggerFactory.getLogger(QParquetHelper.class);

    public static final String USER_LOGS_LAUNCH = "avro/userlogs_launch.avsc";


    /**
     * 根据json形式的元数据描述创建元数据对象
     * @param jsonSchema
     * @return
     */
    public static Schema generateSchema4Json(String jsonSchema) {
        Schema schema = null;
        try{
            if(StringUtils.isNotEmpty(jsonSchema)){
                schema = new Schema.Parser().parse(jsonSchema);
            }
        }catch (Exception e){
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return schema;
    }

    /**
     * 根据avro格式文件创建元数据对象
     * @param path
     * @return
     */
    public static Schema generateSchema4File(String path) {
        Schema schema = null;
        try{
            if(StringUtils.isNotEmpty(path)){
                //基于AVRO文件
                schema = new Schema.Parser().parse(QParquetHelper.class.getClassLoader().getResourceAsStream(path));
            }
        }catch (Exception e){
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return schema;
    }


    /**
     * 根据元数据创造数据
     * @param schema 元数据
     * @return
     */
    public static GenericData.Record generateRecord(Schema schema) {
        GenericData.Record record = null;
        try{
            record = new GenericData.Record(schema);
        }catch (Exception e){
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return record;
    }

    /**
     * 根据元数据创造数据列表
     * @param schema 元数据
     * @return
     */
    public static List<GenericData.Record> generateRecord(Schema schema, int count) {
        List<GenericData.Record> records = new ArrayList<GenericData.Record>();
        try{
            for(int i=1;i<=count;i++){
                GenericData.Record record = new GenericData.Record(schema);
                records.add(record);
            }
        }catch (Exception e){
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return records;
    }

    /**
     * 数据类型转换
     * @param type 数据类型
     * @param value 数据
     * @return
     * @throws Exception
     */
    public static Object convert(String type, Object value) throws Exception{
        Object newValue = new Object();
        if(!StringUtils.isEmpty(type) && null != value){
            if(Schema.Type.STRING.getName().equals(type)){
                newValue = value.toString();
            }else if(Schema.Type.INT.getName().equals(type)){
                newValue = Integer.valueOf(value.toString());
            }else if(Schema.Type.LONG.getName().equals(type)){
                newValue = Integer.valueOf(value.toString());
            }else if(Schema.Type.DOUBLE.getName().equals(type)){
                newValue = Integer.valueOf(value.toString());
            }else if(Schema.Type.FLOAT.getName().equals(type)){
                newValue = Integer.valueOf(value.toString());
            }else if(Schema.Type.ARRAY.getName().equals(type)){
                newValue = Lists.newArrayList(value);
            }
        }
        return newValue;
    }


    public static void main(String[] args) {

        Schema schema = QParquetHelper.generateSchema4File(QParquetHelper.USER_LOGS_LAUNCH);
        System.out.println(schema.toString());
    }


}
