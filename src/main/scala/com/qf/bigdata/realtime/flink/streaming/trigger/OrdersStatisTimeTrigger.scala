package com.qf.bigdata.realtime.flink.streaming.trigger

import java.util.Date
import java.util.concurrent.TimeUnit

import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.OrderDetailData
import com.qf.bigdata.realtime.util.CommonUtil
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.slf4j.{Logger, LoggerFactory}

/**
  * 旅游产品订单业务自定义时间触发器
  * 基于处理时间间隔时长触发任务处理
  */
class OrdersStatisTimeTrigger(maxInterval :Long) extends Trigger[OrderDetailData, TimeWindow]{

  val logger :Logger = LoggerFactory.getLogger("OrdersStatisTimeTrigger")

  //统计数据状态：计数
  val TRIGGER_ORDER_STATE_TIME_DESC = "TRIGGER_ORDER_STATE_TIME_DESC"
  var ordersTimeStateDesc :ValueStateDescriptor[Long] = new ValueStateDescriptor[Long](TRIGGER_ORDER_STATE_TIME_DESC, createTypeInformation[Long])
  var ordersTimeState :ValueState[Long] = _


  /**
    * 元素处理
    * @param element
    * @param timestamp
    * @param window
    * @param ctx
    * @return
    */
  override def onElement(element: OrderDetailData, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //计数状态
    ordersTimeState = ctx.getPartitionedState(ordersTimeStateDesc)

    //处理时间间隔
    val maxIntervalTimestamp :Long = Time.of(maxInterval,TimeUnit.MINUTES).toMilliseconds
    val curProcessTime :Long = ctx.getCurrentProcessingTime

    //当前处理时间到达或超过上次处理时间+间隔后触发本次窗口操作
    var nextProcessingTime = TimeWindow.getWindowStartWithOffset(curProcessTime, 0, maxIntervalTimestamp) + maxIntervalTimestamp
    ctx.registerProcessingTimeTimer(nextProcessingTime)

    ordersTimeState.update(nextProcessingTime)
    return TriggerResult.CONTINUE
  }

  /**
    * 一个已注册的处理时间计时器启动时调用
    * @param time
    * @param window
    * @param ctx
    * @return
    */
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println(s"""OrdersStatisTimeTrigger.onProcessingTime=${CommonUtil.formatDate4Def(new Date())}""")
    return TriggerResult.FIRE;
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.CONTINUE;
  }

  /**
    * 执行任何需要清除的相应窗口
    * @param window
    * @param ctx
    */
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    //删除处理时间的定时器
    ctx.deleteProcessingTimeTimer(ordersTimeState.value())
  }

  /**
    * 合并窗口
    * @param window
    * @param ctx
    */
  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    println(s"""OrdersStatisTimeTrigger.onMerge=${CommonUtil.formatDate4Def(new Date())}""")
    val windowMaxTimestamp = window.maxTimestamp()
    if(windowMaxTimestamp > ctx.getCurrentWatermark){
      ctx.registerProcessingTimeTimer(windowMaxTimestamp)
    }
  }


}
