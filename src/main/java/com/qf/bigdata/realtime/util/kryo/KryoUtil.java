package com.qf.bigdata.sharecar.util.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Administrator on 2017-6-13.
 */
public class KryoUtil implements Serializable{

    private static final int bufCap = 2048;

    /**
     * 对象序列化
     *
     * @param value
     * @return
     */
    public static byte[] serializer(Object value) {
        try {
            Kryo kryo = new Kryo();
            byte[] buffer = new byte[bufCap];
            Output output = new Output(buffer);
            kryo.writeClassAndObject(output, value);
            return output.toBytes();
        } catch (Exception e) {
            return null;
        }
    }
    /**
     * 对象反序列化
     *
     * @param value
     * @return
     */
    public static Object deserializer(byte[] value) {
        try {
            Kryo kryo = new Kryo();
            Input input = new Input(value);
            return kryo.readClassAndObject(input);
        } catch (Exception e) {
            return null;
        }
    }


    //---------------------------------------------------------------------

    public static <T extends Serializable> byte[] serializationObject(T obj) throws Exception{
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.register(obj.getClass(), new JavaSerializer());


        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeClassAndObject(output, obj);
        output.flush();
        output.close();

        byte[] bytes = baos.toByteArray();
        try {
            baos.flush();
            baos.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }

        return bytes;
    }


    public static <T extends Serializable> T deserializationObject(byte[] bytes,
                                                             Class<T> clazz) {
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.register(clazz, new JavaSerializer());

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        Input input = new Input(bais);
        return (T) kryo.readClassAndObject(input);
    }


    public static void main(String[] args) {

//        BikeBattery oldbattery = BikeBattery.createBatteryData4Temp();
//
//        System.out.println("battery.old=>" + oldbattery.toString());
//
//        byte[] values = serializer(oldbattery);
//
//        BikeBattery battery = (BikeBattery)deserializer(values);
//
//        System.out.println("battery.new=>" + battery.toString());



    }
}
