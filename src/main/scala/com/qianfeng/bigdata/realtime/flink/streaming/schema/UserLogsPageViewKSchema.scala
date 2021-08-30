package com.qianfeng.bigdata.realtime.flink.streaming.schema

import com.google.gson.Gson
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.lang

import com.qianfeng.bigdata.realtime.util.{CommonUtil, JsonUtil}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * 行为日志页面浏览数据(明细)kafka序列化
 */
class UserLogsPageViewKSchema(topic:String) extends KafkaSerializationSchema[UserLogPageViewData] with KafkaDeserializationSchema[UserLogPageViewData]{

  /**
   * 反序列化
   * @return
   */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogPageViewData = {
    val value = record.value()
    val gson : Gson = new Gson()
    val log :UserLogPageViewData = gson.fromJson(new String(value), classOf[UserLogPageViewData])
    log
  }

  /**
   * 序列化
   * @param element
   * @return
   */
  override def serialize(element: UserLogPageViewData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val sid = element.sid
    val userDevice = element.userDevice
    val targetID = element.targetID
    val tmp = sid + userDevice+ targetID
    //我之前删除---需要在CommonUtil中加入该方法
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)

    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }

  override def isEndOfStream(nextElement: UserLogPageViewData): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[UserLogPageViewData] = {
    return TypeInformation.of(classOf[UserLogPageViewData])
  }
}