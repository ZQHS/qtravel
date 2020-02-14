package com.qf.bigdata.realtime.flink.streaming.etl.dw.orders

import java.util.Properties

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.OrderDetailKSchema
import com.qf.bigdata.realtime.flink.streaming.assigner.OrdersPeriodicAssigner
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.OrderDetailDataMapFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{OrderDetailData, OrderWideAggDimData, OrderWideData, OrderWideTimeAggDimMeaData}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.slf4j.{Logger, LoggerFactory}

/**
  * 旅游产品订单实时数据处理
  */
object OrdersDetailHandler {

  val logger :Logger = LoggerFactory.getLogger("OrdersDetailHandler")


  /**
    * 实时开窗聚合数据
    */
  def handleOrdersJob(appName:String, fromTopic:String, groupID:String, toTopic:String):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
      env.getConfig.setAutoWatermarkInterval(QRealTimeConstant.FLINK_WATERMARK_INTERVAL)
      env.registerType(classOf[OrderDetailData])


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
      dStream.print("orderDStream---:")

      /**
        * 4 设置事件时间提取器及水位计算
        *   固定范围的水位指定(注意时间单位)
        */
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      orderDetailDStream.assignTimestampsAndWatermarks(ordersPeriodicAssigner)
      orderDetailDStream.print("order.orderDStream---")


      /**
        * 5 订单数据打回kafka为下游环节计算准备数据
        */
      val kafkaSerSchema = new OrderDetailKSchema(toTopic)
      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val travelKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        kafkaSerSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

      // 加入kafka摄入时间
      travelKafkaProducer.setWriteTimestampToKafka(true)
      orderDetailDStream.addSink(travelKafkaProducer)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersDetailHandler.err:" + ex.getMessage)
      }
    }

  }



  /**
    * 实时开窗聚合数据
    */
  def handleOrdersAggJob(appName:String, fromTopic:String, groupID:String, toTopic:String):Unit = {
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
        * 5 设置事件时间提取器及水位计算
        *   固定范围的水位指定(注意时间单位)
        */
        val orderBoundedAssigner = new BoundedOutOfOrdernessTimestampExtractor[OrderDetailData](Time.seconds(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)) {
          override def extractTimestamp(element: OrderDetailData): Long = {
            val ct = element.ct
            ct
          }
        }
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      //orderDetailDStream.print("order.orderDStream---")


      /**
        * 4 订单数据聚合
        */
      val aggDStream:DataStream[_] = orderDetailDStream
        .assignTimestampsAndWatermarks(orderBoundedAssigner)
        .keyBy(_.productID)
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .allowedLateness(Time.seconds(5))
        .sum("price")
      aggDStream.print("order.aggDStream---:")


      /**
        * 5 订单数据打回kafka为下游环节计算准备数据
        */
      //      val kafkaSerSchema = new OrderDetailKSchema(toTopic)
      //      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      //      val travelKafkaProducer = new FlinkKafkaProducer(
      //        toTopic,
      //        kafkaSerSchema,
      //        kafkaProductConfig,
      //        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
      //
      //      // 加入kafka摄入时间
      //      travelKafkaProducer.setWriteTimestampToKafka(true)
      //      orderDetailDStream.addSink(travelKafkaProducer)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersDetailHandler.err:" + ex.getMessage)
      }
    }

  }


  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)

    val appName = "flink.OrdersDetailHandler"
    //val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val fromTopic = "test_ods"

    val toTopic = QRealTimeConstant.TOPIC_ORDER_DW
    val groupID = "group.OrdersDetailHandler2020"

    //handleOrdersJob(appName, fromTopic, groupID, toTopic)


    handleOrdersAggJob(appName:String, fromTopic:String, groupID:String, toTopic:String)


  }

}
