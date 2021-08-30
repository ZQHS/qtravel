package com.qianfeng.bigdata.realtime.flink.streaming.schema


import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import java.lang

import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.OrderWideTimeAggDimMeaData
import com.qianfeng.bigdata.realtime.util.{CommonUtil, JsonUtil}
import org.apache.kafka.clients.producer.ProducerRecord
/**
 * 订单宽表聚合数据序列化
 */
class OrderWideGroupKSchema(topic:String) extends KafkaSerializationSchema[OrderWideTimeAggDimMeaData]{

  override def serialize(element: OrderWideTimeAggDimMeaData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val productType = element.productType
    val toursimType = element.toursimType
    val tmp = productType+ toursimType
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)
    val value = JsonUtil.gObject2Json(element)

    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }
}
