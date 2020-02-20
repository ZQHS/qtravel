package com.qf.bigdata.realtime.flink.streaming.trigger

import java.util.Date

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{OrderDetailData}
import com.qf.bigdata.realtime.util.CommonUtil
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.slf4j.{Logger, LoggerFactory}

/**
  * 旅游产品订单业务自定义数量触发器
  * 基于计数器触发任务处理
  */
class OrdersStatisCountTrigger(maxCount:Long) extends Trigger[OrderDetailData, TimeWindow]{

  val logger :Logger = LoggerFactory.getLogger("OrdersStatisCountTrigger")

  //统计数据状态：计数
  val TRIGGER_ORDER_STATE_ORDERS_DESC = "TRIGGER_ORDER_STATE_ORDERS_DESC"
  var ordersCountStateDesc :ValueStateDescriptor[Long] = new ValueStateDescriptor[Long](TRIGGER_ORDER_STATE_ORDERS_DESC, createTypeInformation[Long])
  var ordersCountState :ValueState[Long] = _



  /**
    * 每条数据被添加到窗口时调用
    * @param element
    * @param timestamp
    * @param window
    * @param ctx
    * @return
    */
  override def onElement(element: OrderDetailData, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //计数状态
    ordersCountState = ctx.getPartitionedState(ordersCountStateDesc)

    //当前数据
    if(ordersCountState.value() == 0){
        ordersCountState.update(QRealTimeConstant.COMMON_NUMBER_ZERO)
    }
    val curOrders = ordersCountState.value() + 1
    ordersCountState.update(curOrders)

    //触发条件判断
    if(curOrders >= maxCount ){
      this.clear(window, ctx)
      return TriggerResult.FIRE_AND_PURGE
    }

    return TriggerResult.CONTINUE
  }




  /**
    * ,当一个已注册的事件时间计时器启动时调用
    * @param time
    * @param window
    * @param ctx
    * @return
    */
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.CONTINUE;
  }

  /**
    * 一个已注册的处理时间计时器启动时调用
    * @param time
    * @param window
    * @param ctx
    * @return
    */
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.FIRE
  }

  /**
    * 执行任何需要清除的相应窗口
    * @param window
    * @param ctx
    */
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    //计数清零
    ctx.getPartitionedState(ordersCountStateDesc).clear()

    //删除处理时间的定时器
    ctx.deleteProcessingTimeTimer(window.maxTimestamp())
  }

  override def canMerge: Boolean = {return true}

  /**
    * 合并窗口
    * @param window
    * @param ctx
    */
  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    println(s"""OrdersStatisCountTrigger.onMerge=${CommonUtil.formatDate4Def(new Date())}""")

    val windowMaxTimestamp :Long = window.maxTimestamp();
    if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
      ctx.registerProcessingTimeTimer(windowMaxTimestamp);
    }
  }
}
