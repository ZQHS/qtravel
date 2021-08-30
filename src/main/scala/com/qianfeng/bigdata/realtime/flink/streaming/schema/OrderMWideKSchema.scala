package com.qianfeng.bigdata.realtime.flink.streaming.schema

import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.OrderMWideData
import com.qianfeng.bigdata.realtime.util.{CommonUtil, JsonUtil}
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import java.lang

/**
 * 订单多维宽表数据
 *
 * @param topic
 */
class OrderMWideKSchema(topic:String) extends KafkaSerializationSchema[OrderMWideData]{

  override def serialize(element: OrderMWideData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val orderID = element.orderID
    val productID = element.productID
    val ct = element.ct
    val tmp = orderID+ productID + element.pubID +ct
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)
    val value = JsonUtil.gObject2Json(element)

    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }
}
