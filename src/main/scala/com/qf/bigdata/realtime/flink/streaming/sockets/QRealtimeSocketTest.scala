package com.qf.bigdata.realtime.flink.streaming.sockets

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.CommonUtil
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import scala.collection.JavaConversions._

object QRealtimeSocketTest {

  /**
    * 计数触发器
    */
  class QCounterTrigger(maxCount :Long) extends Trigger[(String,Int), TimeWindow]{

    //统计数据状态：计数
    val COUNTER_DESC = "COUNTER_DESC"
    var countStateDesc :ValueStateDescriptor[Long] = new ValueStateDescriptor[Long](COUNTER_DESC, createTypeInformation[Long])
    var countState :ValueState[Long] = _

    /**
      * 每条数据被添加到窗口时调用
      */
    override def onElement(element: (String,Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      //计数状态
      countState = ctx.getPartitionedState(countStateDesc)
      val ordersCount :Long = countState.value()

      //初始化
      if(ordersCount == null){
        countState.update(QRealTimeConstant.COMMON_NUMBER_ZERO)
        return TriggerResult.CONTINUE
      }

      //即时数据
      val curOrders = ordersCount + 1
      if(curOrders >= maxCount ){
        ctx.registerProcessingTimeTimer(window.maxTimestamp)
        return TriggerResult.FIRE
      }else{
        countState.update(curOrders)
      }
      return TriggerResult.CONTINUE
    }

    /**
      * ,当一个已注册的事件时间计时器启动时调用
      */
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      return TriggerResult.CONTINUE;
    }

    /**
      * 一个已注册的处理时间计时器启动时调用
      */
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      countState = ctx.getPartitionedState(countStateDesc)
      val ordersCount :Long = countState.value()
      val start :Long = window.getStart
      val startTime = CommonUtil.formatDate4Timestamp(start, QRealTimeConstant.FORMATTER_YYYYMMDDHHMMSS)
      val end :Long = window.getEnd
      val endTime = CommonUtil.formatDate4Timestamp(start, QRealTimeConstant.FORMATTER_YYYYMMDDHHMMSS)
      val maxTimestamp :Long = window.maxTimestamp()
      val maxTime = CommonUtil.formatDate4Timestamp(maxTimestamp, QRealTimeConstant.FORMATTER_YYYYMMDDHHMMSS)
      println(s"""onProcessingTime start=${startTime}，end=${endTime}, count=${ordersCount}, maxTime=${maxTime}""")

      return TriggerResult.FIRE
    }

    /**
      * 执行任何需要清除的相应窗口
      */
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      //删除事件时间的定时器
      ctx.deleteProcessingTimeTimer(window.maxTimestamp())

      //计数清零
      ctx.getPartitionedState(countStateDesc).clear()
    }

    override def canMerge: Boolean = {return true}



  }


  /**
    * 实时开窗聚合数据
    */
  def test():Unit = {

    //1 flink环境初始化
    val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
    //使用事件时间做处理参考
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    case class MyRec(world:String,acc:Int)

    //2 kafka流式数据源
    val dStream:DataStream[String] = env.socketTextStream("127.0.0.1", 18080)
    val mStream :DataStream[(String,Int)]= dStream.map(r => (r,1))
    //mStream.print()

    //计数触发器
    val countTrigger :Trigger[(String,Int),TimeWindow] = new QCounterTrigger(5)

    val aggDStream:DataStream[(String,Int)] = mStream.keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.minutes(QRealTimeConstant.FLINK_WINDOW_MAX_SIZE)))
      .trigger(countTrigger)
      .sum(1)
    aggDStream.print()

    env.execute("flink.window.test")
  }


  /**
    * 实时开窗聚合数据
    */
  def testLocal():Unit = {

    case class MyUser(num:String, gender:String, ct:Long)

    //1 flink环境初始化
    val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
    //使用事件时间做处理参考
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.registerType(classOf[MyUser])

    println(Long.MinValue)


    var users :Seq[(String, Int,Long)] = Seq[(String,Int,Long)]()
    for(idx <- 1 to 100){
      val num = CommonUtil.getRandom(1)
      val ct :Long = CommonUtil.getRandomTimestamp
      users = users.:+((num, idx, ct))
    }

    //2 数据结构数据源
    val dStream:DataStream[(String,Int,Long)] = env.fromCollection(users)
    val userBoundedAssigner = new BoundedOutOfOrdernessTimestampExtractor[(String,Int,Long)](Time.seconds(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)) {
      override def extractTimestamp(element: (String,Int,Long)): Long = {
        val ct = element._3
        println(s"""ct=$ct""")
        ct
      }
    }
    val dStream2:DataStream[(String,Int,Long)] = dStream.assignTimestampsAndWatermarks(userBoundedAssigner)
    //dStream2.print("dStream")

    /**
      * 4 订单数据聚合
      */
    val aggDStream:DataStream[_] = dStream2.keyBy(_._2)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      //.allowedLateness(Time.seconds(5))
      .sum(1)
    aggDStream.print("aggDStream---:")

    env.execute("flink.et.test")
  }


  def main(args: Array[String]): Unit = {

    //test()


    testLocal()

  }

}
