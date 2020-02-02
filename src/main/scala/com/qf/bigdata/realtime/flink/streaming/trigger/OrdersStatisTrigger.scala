package com.qf.bigdata.realtime.flink.streaming.trigger

import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{OrderDetailData}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult

/**
  * 旅游产品订单业务自定义触发器
  */
class OrdersStatisTrigger(maxCount:Long, maxInterval :Long) extends Trigger[OrderDetailData, TimeWindow]{

  val logger :Logger = LoggerFactory.getLogger("OrdersAggTrigger")

  var accCount:Long = 1l

  override def onElement(element: OrderDetailData, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    logger.info("======OrdersStatisTrigger.onElement====window start = {}, window end = {}", window.getStart, window.getEnd)

    ctx.registerEventTimeTimer(window.maxTimestamp)
    if(accCount >= maxCount ){
      accCount = 0
      return TriggerResult.FIRE
    }else{
      accCount = accCount + 1
    }
    return TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.FIRE;
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.CONTINUE;
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.deleteProcessingTimeTimer(window.maxTimestamp)
  }

}
