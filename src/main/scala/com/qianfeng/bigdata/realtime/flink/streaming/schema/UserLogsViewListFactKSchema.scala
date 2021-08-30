package com.qianfeng.bigdata.realtime.flink.streaming.schema

import com.google.gson.Gson
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qianfeng.bigdata.realtime.util.{CommonUtil, JsonUtil}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang

/**
 * 行为日志产品列表浏览数据(明细)kafka序列化
 */
class UserLogsViewListFactKSchema(topic:String) extends KafkaSerializationSchema[UserLogViewListFactData]
  with KafkaDeserializationSchema[UserLogViewListFactData] {
  /**
   * 反序列化
   *
   * @return
   */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogViewListFactData = {
    val key = record.key()
    val value = record.value()
    val gson: Gson = new Gson()
    val log: UserLogViewListFactData = gson.fromJson(new String(value), classOf[UserLogViewListFactData])
    log
  }

  /**
   * 序列化
   *
   * @param element
   * @return
   */
  override def serialize(element: UserLogViewListFactData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val sid = element.sid
    val userDevice = element.userDevice
    val userID = element.userID
    val tmp = sid + userDevice + userID
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)
    //获取value
    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }

  override def isEndOfStream(nextElement: UserLogViewListFactData): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[UserLogViewListFactData] = {
    return TypeInformation.of(classOf[UserLogViewListFactData])
  }

}