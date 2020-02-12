package com.qf.bigdata.realtime.flink.schema

import java.lang

import com.google.gson.Gson
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogPageViewAggMeanData}
import com.qf.bigdata.realtime.util.json.JsonUtil
import com.qf.bigdata.realtime.util.uid.QUIDService
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 行为日志页面浏览数据（聚合后）kafka序列化
  */
class UserLogsPageViewAggKSchema(topic:String) extends KafkaSerializationSchema[UserLogPageViewAggMeanData] with KafkaDeserializationSchema[UserLogPageViewAggMeanData]{





  /**
    * 反序列化
    * @param message
    * @return
    */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogPageViewAggMeanData = {
    val key = record.key()
    val value = record.value()
    val gson : Gson = new Gson()
    val log :UserLogPageViewAggMeanData = gson.fromJson(new String(value), classOf[UserLogPageViewAggMeanData])
    log
  }

  /**
    * 序列化
    * @param element
    * @return
    */
  override def serialize(element: UserLogPageViewAggMeanData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val access = element.access
    val users = element.users
    val durations = element.totalDuration
    val quidService: QUIDService = new QUIDService(access, users, durations)
    val key : String = quidService.nextId().toString

    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }

  override def isEndOfStream(nextElement: UserLogPageViewAggMeanData): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[UserLogPageViewAggMeanData] = {
    return TypeInformation.of(classOf[UserLogPageViewAggMeanData])
  }

}
