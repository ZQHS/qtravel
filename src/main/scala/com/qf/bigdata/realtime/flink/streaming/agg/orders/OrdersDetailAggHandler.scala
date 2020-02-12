package com.qf.bigdata.realtime.flink.streaming.agg.orders

import java.util.Properties

import com.qf.bigdata.realtime.constant.CommonConstant
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qf.bigdata.realtime.flink.streaming.assigner.OrdersPeriodicAssigner
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersAggFun.{OrderDetailTimeAggFun, OrderDetailTimeWindowFun}
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.OrderDetailDataMapFun
import com.qf.bigdata.realtime.flink.streaming.sink.CommonESSink
import com.qf.bigdata.realtime.flink.streaming.trigger.OrdersStatisTrigger
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import com.qf.bigdata.realtime.util.json.JsonUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * 旅游订单业务实时计算
  */
object OrdersDetailAggHandler {

  val logger :Logger = LoggerFactory.getLogger("OrdersDetailAggHandler")

  /**
    * 实时开窗聚合数据
    */
  def handleOrdersAggWindowJob(appName:String, fromTopic:String, toTopic:String, groupID:String, indexName:String):Unit = {

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

      //kafka消费+消费策略
      val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, consumerProperties)
      kafkaConsumer.setStartFromLatest()
      kafkaConsumer.setCommitOffsetsOnCheckpoints(true)

      /**
        * 3 订单数据
        *   原始明细数据转换操作
        */
      val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      val orderDetailDStream :DataStream[OrderDetailData] = dStream.map(new OrderDetailDataMapFun())
      //orderDetailDStream.print("orderDStream---:")


      /**
        * 4 开窗聚合操作
        */
      val aggDStream:DataStream[OrderDetailTimeAggDimMeaData] = orderDetailDStream.keyBy(
        (detail:OrderDetailData) => OrderDetailAggDimData(detail.userRegion, detail.traffic)
      )
        .window(TumblingProcessingTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE)))
        .allowedLateness(Time.seconds(QRealTimeConstant.FLINK_ALLOWED_LATENESS))
        .aggregate(new OrderDetailTimeAggFun(), new OrderDetailTimeWindowFun())
      aggDStream.print("order.aggDStream---:")


      /**
        * 5 聚合数据写入ES
        */
      val esDStream:DataStream[String] = aggDStream.map(
        (value : OrderDetailTimeAggDimMeaData) => {
          val result :mutable.Map[String,AnyRef] = JsonUtil.gObject2Map(value)
          val eid = value.userRegion+CommonConstant.BOTTOM_LINE+value.traffic
          result +=(QRealTimeConstant.KEY_ES_ID -> eid)
          JsonUtil.gObject2Json(result)
        }
      )


      /**
        * 6 数据输出Sink
        *   自定义ESSink输出
        */
      val orderAggESSink = new CommonESSink(indexName)
      //esDStream.addSink(orderAggESSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersDetailAggHandler.err:" + ex.getMessage)
      }
    }

  }



  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)

    val appName = "qf.OrdersDetailAggHandler"
    val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS

    val toTopic = QRealTimeConstant.TOPIC_ORDER_DM
    val groupID = "group.OrdersDetailAggHandler"
    val indexName = QRealTimeConstant.ES_INDEX_NAME_ORDER_WIN_STATIS



    //实时处理第二层：开窗统计
    handleOrdersAggWindowJob(appName, fromTopic, toTopic, groupID, indexName)


  }


}
