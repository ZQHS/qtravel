package com.qf.bigdata.realtime.flink.schema

import java.lang

import com.google.gson.Gson
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.UserLogData
import com.qf.bigdata.realtime.util.json.JsonUtil
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 行为日志kafka序列化
  */
class UserLogsKSchema(topic:String) extends KafkaSerializationSchema[UserLogData] with KafkaDeserializationSchema[UserLogData] {

  val gson : Gson = new Gson()

  /**
    * 反序列化
    * @param message
    * @return
    */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogData = {
    val key = record.key()
    val value = record.value()
    val log :UserLogData = gson.fromJson(new String(value), classOf[UserLogData])
    log
  }

  /**
    * 序列化
    * @param element
    * @return
    */
  override def serialize(element: UserLogData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val key = element.sid
    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }

  override def isEndOfStream(nextElement: UserLogData): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[UserLogData] = {
    return TypeInformation.of(classOf[UserLogData])
  }




}
