package com.qianfeng.bigdata.realtime.flink.streaming.schema

import java.lang

import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{OrderWideData}
import com.qianfeng.bigdata.realtime.util.CommonUtil
import com.qianfeng.bigdata.realtime.util.JsonUtil
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 订单宽表数据
  * @param topic
  */
class OrderWideKSchema (topic:String) extends KafkaSerializationSchema[OrderWideData]{

  override def serialize(element: OrderWideData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val orderID = element.orderID
    val productID = element.productID
    val ct = element.ct
    val tmp = orderID+ productID+ ct
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)
    val value = JsonUtil.gObject2Json(element)

    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }
}
