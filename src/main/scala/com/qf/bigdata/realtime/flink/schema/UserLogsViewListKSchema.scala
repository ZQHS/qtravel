package com.qf.bigdata.realtime.flink.schema

import java.lang

import com.google.gson.Gson
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogViewListData}
import com.qf.bigdata.realtime.util.CommonUtil
import com.qf.bigdata.realtime.util.json.JsonUtil
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 行为日志产品列表浏览数据(原始)kafka序列化
  */
class UserLogsViewListKSchema(topic:String) extends KafkaSerializationSchema[UserLogViewListData] with KafkaDeserializationSchema[UserLogViewListData] {



  val gson : Gson = new Gson()


  /**
    * 反序列化
    * @param message
    * @return
    */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogViewListData = {
    val key = record.key()
    val value = record.value()
    val log :UserLogViewListData = gson.fromJson(new String(value), classOf[UserLogViewListData])
    log
  }

  /**
    * 序列化
    * @param element
    * @return
    */
  override def serialize(element: UserLogViewListData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val sid = element.sid
    val userDevice = element.userDevice
    val userID = element.userID
    val tmp = sid + userDevice+ userID
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)

    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }

  override def isEndOfStream(nextElement: UserLogViewListData): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[UserLogViewListData] = {
    return TypeInformation.of(classOf[UserLogViewListData])
  }


}
