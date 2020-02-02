package com.qf.bigdata.realtime.flink.streaming.agg.orders

import java.util.{Date, Properties}

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.{OrderStatisKSchema}
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersAggFun._
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.OrderDetailDataMapFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qf.bigdata.realtime.flink.streaming.sink.orders.{OrderStatisESSink}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._


/**
  * 旅游订单业务实时排名
  * 订单数据综合统计排名
  */
object OrdersTopnStatisHandler {

  val logger :Logger = LoggerFactory.getLogger("OrdersTopnStatisHandler")


  /**
    * 实时数据综合统计
    * 基于数据量作为触发条件进行窗口分组聚合
    */
  def handleOrdersWindowTopNJob(appName:String, fromTopic:String, toTopic:String, groupID:String, topN:Long, maxInternal:Long):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
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

      /**
        * 3 订单数据
        *   原始明细数据转换操作
        */
      val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      val orderDetailDStream :DataStream[OrderDetailData] = dStream.map(new OrderDetailDataMapFun())
      //orderDetailDStream.print("orderDStream---:")

      /**
        * 4 设置事件时间提取器及水位计算
        *   固定范围的水位指定(注意时间单位)
        */
      val orderBoundedAssigner = new BoundedOutOfOrdernessTimestampExtractor[OrderDetailData](Time.milliseconds(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)) {
        override def extractTimestamp(element: OrderDetailData): Long = {
          element.ct
        }
      }

      //周期间隔
      //val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      orderDetailDStream.assignTimestampsAndWatermarks(orderBoundedAssigner)
      //orderDetailDStream.print("order.orderDStream---")

      /**
        * 5 开窗聚合操作
        */
      val aggDStream:DataStream[OrderTrafficDimMeaData] = orderDetailDStream.keyBy(
        (detail:OrderDetailData) => {
          OrderTrafficDimData(detail.productID, detail.traffic)
        }
      )
        .window(TumblingEventTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE)))
        .allowedLateness(Time.seconds(QRealTimeConstant.FLINK_ALLOWED_LATENESS))
        .aggregate(new OrderDetailTimeAggFun(), new OrderTrafficTimeWindowFun())
      aggDStream.print("order.aggDStream---:")


      /**
        * 6 topN排序
        * Sorted的数据结构TreeSet或者是优先级队列PriorityQueue , TreeSet 实现原理是红黑树，优先队列实现原理就是最大/最小堆，
        * 这两个都可以满足需求，但要如何选择？红黑树的时间复杂度是logN，而堆的构造复杂度是N, 读取复杂度是1，
        * 但是我们这里需要不断的做数据插入那么就涉及不断的构造过程，相对而言选择红黑树比较好(其实flink sql内部做topN也是选择红黑树类型的TreeMap)
        */
      val topNDStream:DataStream[OrderTrafficDimMeaData] = aggDStream.keyBy(
        (detail:OrderTrafficDimMeaData) => {
          OrderTrafficDimData(detail.productID, detail.traffic)
        }
      )
        .window(TumblingEventTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE)))
        .allowedLateness(Time.seconds(QRealTimeConstant.FLINK_ALLOWED_LATENESS))
        .process(new OrderTopNKeyedProcessFun(topN))
      topNDStream.print("order.topNDStream---:")

      /**
        * 7 数据输出kafka
        */
      val orderStatisKSchema = new OrderStatisKSchema(toTopic)
      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val travelKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        orderStatisKSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

      //5 加入kafka摄入时间
      travelKafkaProducer.setWriteTimestampToKafka(true)
      topNDStream.addSink(travelKafkaProducer)



      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersTopnStatisHandler.err:" + ex.getMessage)
      }
    }

  }




  /**
    * 实时数据综合统计
    * 基于数据量作为触发条件进行窗口分组聚合
    */
  def handleOrdersWindowTopN4ESJob(appName:String, fromTopic:String, indexName:String, groupID:String, topN:Long, maxInternal:Long):Unit = {

    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
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

      /**
        * 3 订单数据
        *   原始明细数据转换操作
        */
      val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      val orderDetailDStream :DataStream[OrderDetailData] = dStream.map(new OrderDetailDataMapFun())
      //orderDetailDStream.print("orderDStream---:")

      /**
        * 4 设置事件时间提取器及水位计算
        *   固定范围的水位指定(注意时间单位)
        */
      val orderBoundedAssigner = new BoundedOutOfOrdernessTimestampExtractor[OrderDetailData](Time.milliseconds(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)) {
        override def extractTimestamp(element: OrderDetailData): Long = {
          element.ct
        }
      }

      //周期间隔
      //val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      orderDetailDStream.assignTimestampsAndWatermarks(orderBoundedAssigner)
      //orderDetailDStream.print("order.orderDStream---")

      /**
        * 5 开窗聚合操作
        */
      val aggDStream:DataStream[OrderTrafficDimMeaData] = orderDetailDStream.keyBy(
        (detail:OrderDetailData) => {
          OrderTrafficDimData(detail.productID, detail.traffic)
        }
      )
        .window(TumblingEventTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE)))
        .allowedLateness(Time.seconds(QRealTimeConstant.FLINK_ALLOWED_LATENESS))
        .aggregate(new OrderDetailTimeAggFun(), new OrderTrafficTimeWindowFun())
      aggDStream.print("order.aggDStream---:")


      /**
        * 6 topN排序
        * Sorted的数据结构TreeSet或者是优先级队列PriorityQueue , TreeSet 实现原理是红黑树，优先队列实现原理就是最大/最小堆，
        * 这两个都可以满足需求，但要如何选择？红黑树的时间复杂度是logN，而堆的构造复杂度是N, 读取复杂度是1，
        * 但是我们这里需要不断的做数据插入那么就涉及不断的构造过程，相对而言选择红黑树比较好(其实flink sql内部做topN也是选择红黑树类型的TreeMap)
        */
      val topNDStream:DataStream[OrderTrafficDimMeaData] = aggDStream.keyBy(
        (detail:OrderTrafficDimMeaData) => {
          OrderTrafficDimData(detail.productID, detail.traffic)
        }
      )
        .window(TumblingEventTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE)))
        .allowedLateness(Time.seconds(QRealTimeConstant.FLINK_ALLOWED_LATENESS))
        .process(new OrderTopNKeyedProcessFun(topN))
      topNDStream.print("order.topNDStream---:")

      /**
        * 7 数据输出ES
        *   自定义输出
        */
      val orderStatisESSink = new OrderStatisESSink(indexName)
      topNDStream.addSink(orderStatisESSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersTopnStatisHandler.err:" + ex.getMessage)
      }
    }


  }




  /**
    * 实时数据综合统计
    * 基于数据量作为触发条件进行窗口分组聚合
    */
  def handleOrdersWindowTopN4FileJob(appName:String, fromTopic:String, groupID:String, targetPath:String,maxInternal:Long):Unit = {

    /**
      * 1 Flink环境初始化
      *   流式处理的时间特征依赖(使用事件时间)
      */
    val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
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

    /**
      * 3 订单数据
      * 原始明细数据转换操作
      */
    val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
    val orderDetailDStream :DataStream[OrderDetailData] = dStream.map(new OrderDetailDataMapFun())
    orderDetailDStream.print("orderDStream---:")
    orderDetailDStream.writeAsCsv(targetPath)
    //orderDetailDStream.writeAsCsv("file:///D:\\qfBigWorkSpace\\qtravel\\src\\main\\resources\\data",FileSystem.WriteMode.NO_OVERWRITE)
    env.execute(appName)
  }




  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)
    val appName = "qf.OrdersTopnStatisHandler"
    val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val toTopic = QRealTimeConstant.TOPIC_ORDER_DM_STATIS
    val groupID = "group.OrdersTopnStatisHandler"


    //定时触发窗口计算
    val maxInternal = 1
    val topN = 10l
    handleOrdersWindowTopNJob(appName, fromTopic, toTopic, groupID, topN,maxInternal)


  }


}
