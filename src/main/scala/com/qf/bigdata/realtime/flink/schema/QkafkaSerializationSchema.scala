package com.qf.bigdata.realtime.flink.schema

import java.lang

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.util.json.{JsonMapperUtil, JsonUtil}
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.JavaConversions._

/**
  * kafka sink 序列化元数据
  */
class QkafkaSerializationSchema(topic:String) extends KafkaSerializationSchema[String]{

  override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {

    val record :java.util.Map[String,String] = JsonMapperUtil.readValue(element, classOf[java.util.Map[String,String]])

    //创建key
    val sid = record.getOrElse(QRealTimeConstant.KEY_SID,"").toString
    val device = record.getOrElse(QRealTimeConstant.KEY_USER_DEVICE,"").toString
    val key = sid + device

    val value : String = JsonUtil.object2json4DefDateFormat(element)
    println(s"""value=${value}""")

    //val value : String = JSON.toJSONString()
    //val value: String = gson.toJson(element)

    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }
}
