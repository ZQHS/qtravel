package com.qf.bigdata.realtime.flink.streaming.agg.orders

import java.util.{Date, Properties}

import com.qf.bigdata.realtime.constant.CommonConstant
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersAggFun.OrderStatisWindowProcessFun
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun._
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qf.bigdata.realtime.flink.streaming.sink.CommonESSink
import com.qf.bigdata.realtime.flink.streaming.trigger.{OrdersStatisCountTrigger, OrdersStatisTimeTrigger}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.json.JsonUtil
import com.qf.bigdata.realtime.util.{CommonUtil, PropertyUtil}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 旅游订单业务实时计算
  * 订单数据综合统计
  */
object OrdersStatisHandler {

  val logger :Logger = LoggerFactory.getLogger("OrdersStatisHandler")


  /**
    * 实时数据综合统计
    * 基于数据量作为触发条件进行窗口分组聚合
    */
  def handleOrdersStatis4CountJob(appName:String, fromTopic:String, toTopic:String, groupID:String, indexName:String,maxCount:Long):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
      env.getConfig.setAutoWatermarkInterval(QRealTimeConstant.FLINK_WATERMARK_INTERVAL)

      /**
        * 2 kafka流式数据源
        *   kafka消费配置参数
        *   kafka消费策略
        */
      val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
      consumerProperties.setProperty("group.id", groupID)

      val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, consumerProperties)
      kafkaConsumer.setStartFromLatest()
      kafkaConsumer.setCommitOffsetsOnCheckpoints(true)

      /**
        * 3 订单数据
        *   原始明细数据转换操作
        */
      val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      val orderDetailDStream :DataStream[OrderDetailData] = dStream.map(new OrderDetailDataMapFun())
      //orderDetailDStream.print(s"order.orderDetailDStream[${CommonUtil.formatDate4Def(new Date())}]---:")


      /**
        * 5 综合统计
        */
      val statisDStream:DataStream[OrderDetailStatisData] = orderDetailDStream.keyBy(
        (detail:OrderDetailData) => {
          //val hourTime2 = TimeWindow.getWindowStartWithOffset(detail.ct, 0, Time.minutes(30).toMilliseconds)
          //println(s"""hourTime2=${hourTime2}, hourTime2Str=${CommonUtil.formatDate4Timestamp(hourTime2, QRealTimeConstant.FORMATTER_YYYYMMDDHHMMSS)}""")
          val hourTime = CommonUtil.formatDate4Timestamp(detail.ct, QRealTimeConstant.FORMATTER_YYYYMMDDHH)
          OrderDetailSessionDimData(detail.traffic, hourTime)
        }
      )
        .window(TumblingProcessingTimeWindows.of(Time.minutes(QRealTimeConstant.FLINK_WINDOW_MAX_SIZE)))
        .trigger(new OrdersStatisCountTrigger(maxCount))
        .process(new OrderStatisWindowProcessFun())
      statisDStream.print(s"order.statisDStream[${CommonUtil.formatDate4Def(new Date())}]---:")

      /**
        * 6 聚合数据写入ES
        */
      val esDStream:DataStream[String] = statisDStream.map(
        (value : OrderDetailStatisData) => {
          val result :java.util.Map[String,Object] = JsonUtil.gObject2Map(value)
          val eid = value.traffic+CommonConstant.BOTTOM_LINE+value.etTime
          result.put(QRealTimeConstant.KEY_ES_ID, eid)
          val addJson = JsonUtil.object2json(result)
          addJson
        }
      )
      esDStream.print("order.esDStream---")

      /**
        * 7 数据输出Sink
        *   自定义ESSink输出
        */
      val orderWideDetailESSink = new CommonESSink(indexName)
      esDStream.addSink(orderWideDetailESSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersStatisHandler.err:" + ex.getMessage)
      }
    }

  }



  /**
    * 实时数据综合统计
    * 基于数据量作为触发条件进行窗口分组聚合
    */
  def handleOrdersStatis4ProcceTimeJob(appName:String, fromTopic:String, toTopic:String, groupID:String, indexName:String,maxInternal:Long):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
      env.getConfig.setAutoWatermarkInterval(QRealTimeConstant.FLINK_WATERMARK_INTERVAL)

      /**
        * 2 kafka流式数据源
        *   kafka消费配置参数
        *   kafka消费策略
        */
      val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
      consumerProperties.setProperty("group.id", groupID)
      val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, consumerProperties)
      kafkaConsumer.setStartFromLatest()
      kafkaConsumer.setCommitOffsetsOnCheckpoints(true)

      /**
        * 3 订单数据
        *   原始明细数据转换操作
        */
      val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      val orderDetailDStream :DataStream[OrderDetailData] = dStream.map(new OrderDetailDataMapFun())
      //orderDetailDStream.print(s"order.orderDetailDStream[${CommonUtil.formatDate4Def(new Date())}]---:")


      /**
        * 4 综合统计
        */
      val statisDStream:DataStream[OrderDetailStatisData] = orderDetailDStream.keyBy(
        (detail:OrderDetailData) => {
          val hourTime = CommonUtil.formatDate4Timestamp(detail.ct, QRealTimeConstant.FORMATTER_YYYYMMDDHH)
          OrderDetailSessionDimData(detail.traffic, hourTime)
        }
      )
        .window(TumblingProcessingTimeWindows.of(Time.days(QRealTimeConstant.COMMON_NUMBER_ONE),  Time.hours(-8))) //每天5分钟触发
        .trigger(new OrdersStatisTimeTrigger(maxInternal))
        .process(new OrderStatisWindowProcessFun())
      statisDStream.print(s"order.statisDStream[${CommonUtil.formatDate4Def(new Date())}]---:")

      /**
        * 6 聚合数据写入ES
        */
      val esDStream:DataStream[String] = statisDStream.map(
        (value : OrderDetailStatisData) => {
          val result :java.util.Map[String,Object] = JsonUtil.gObject2Map(value)
          val eid = value.traffic+CommonConstant.BOTTOM_LINE+value.etTime
          result.put(QRealTimeConstant.KEY_ES_ID, eid)
          val addJson = JsonUtil.object2json(result)
          addJson
        }
      )
      esDStream.print("order.esDStream---")

      /**
        * 7 数据输出Sink
        *   自定义ESSink输出
        */
      val orderWideDetailESSink = new CommonESSink(indexName)
      esDStream.addSink(orderWideDetailESSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersStatisHandler.err:" + ex.getMessage)
      }
    }

  }


  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)
    val appName = "qf.OrdersStatisHandler"
    val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS

    val toTopic = QRealTimeConstant.TOPIC_ORDER_DM_STATIS
    val groupID = "group.OrdersStatisHandler"


    //定量触发窗口计算
//    val maxCount = 500
//    val indexName = "travel_orders_count_statis"
//    handleOrdersStatis4CountJob(appName, fromTopic, toTopic, groupID, indexName,maxCount)

    //定时触发窗口计算
//    val maxInternal = 1
//    val indexName = "travel_orders_time_statis"
//    handleOrdersStatis4ProcceTimeJob(appName, fromTopic, toTopic, groupID, indexName,maxInternal)


  }

}
