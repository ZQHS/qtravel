package com.qianfeng.bigdata.realtime.flink.streaming.trigger

import java.util.Date

import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.OrderDetailData
import com.qianfeng.bigdata.realtime.util.CommonUtil
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 根据订单条数来触发任务
 */
class OrdersStaticCountTrigger(maxCount:Long) extends Trigger[OrderDetailData,TimeWindow]{

  //定义一个计算器
  val ordersStateDescName = "ORDER_COUNT_TRIGGER"
  var ordersCountState:ValueState[Long] = _
  //需要引入createTypeInfomation包
  // 定义描述器
  val ordersCountStateDesc: ValueStateDescriptor[Long] = new ValueStateDescriptor[Long](
    ordersStateDescName, createTypeInformation[Long])
  //val ordersCountStateDesc: ValueStateDescriptor[Long] = new ValueStateDescriptor[Long](ordersStateDescName, TypeInformation.of(new TypeHint[Long] {}))

  //每个元素执行一次
  override def onElement(element: OrderDetailData,
                         timestamp: Long,
                         window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = {
    //计数状态
    ordersCountState = ctx.getPartitionedState(ordersCountStateDesc)

    //当前数据
    if(ordersCountState.value() == 0){
      ordersCountState.update(QRealTimeConstant.COMMON_NUMBER_ZERO)
    }
    // 累加元素并更新状态
    val curOrders = ordersCountState.value() + 1
    ordersCountState.update(curOrders)

    //触发条件判断
    if(curOrders >= maxCount ){
      this.clear(window, ctx)
      TriggerResult.FIRE  //触发并清空
    } else {
      //继续，不触发
      TriggerResult.CONTINUE
    }
  }

  //基于处理时间处理一次
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE
  }

  //基于实践时间
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.CONTINUE
  }

  //清空窗口数据
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    //计数清零
    ctx.getPartitionedState(ordersCountStateDesc).clear()

    //删除处理时间的定时器
    ctx.deleteProcessingTimeTimer(window.maxTimestamp())
  }

  override def canMerge: Boolean = {return true}

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    println(s"""OrdersStatisCountTrigger.onMerge=${CommonUtil.formatDate4Def(new Date())}""")

    val windowMaxTimestamp :Long = window.maxTimestamp();
    if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
      //注册时间触发器
      ctx.registerProcessingTimeTimer(windowMaxTimestamp);
    }
  }
}
