package com.qf.bigdata.realtime.flink.streaming.agg.selector

import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{OrderWideData}
import org.apache.flink.api.java.functions.KeySelector

/**
  * 订单分组key选择器
  */
class OrdersSelector extends KeySelector[OrderWideData,(String,String)]{
  override def getKey(value: OrderWideData): (String,String) = {
    (value.productType, value.toursimType)
  }
}
