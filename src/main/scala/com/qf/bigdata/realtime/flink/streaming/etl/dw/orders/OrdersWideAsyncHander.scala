package com.qf.bigdata.realtime.flink.streaming.etl.dw.orders

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.OrderWideKSchema
import com.qf.bigdata.realtime.flink.streaming.assigner.OrdersPeriodicAssigner
import com.qf.bigdata.realtime.flink.streaming.etl.dw.orders.OrdersWideDetail2ESHandler.logger
import com.qf.bigdata.realtime.flink.streaming.funs.common.QRealtimeCommFun._
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun._
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{OrderDetailData, OrderMWideData, OrderWideData}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * 定时同步维表数据
  * 异步执行
  */
object OrdersWideAsyncHander {

  val logger :Logger = LoggerFactory.getLogger("OrdersWideAsyncHander")


  /**
    * 构造旅游产品数据查询对象
    */
  def createProductDBQuery():DBQuery = {
    val sql = QRealTimeConstant.SQL_PRODUCT
    val schema = QRealTimeConstant.SCHEMA_PRODUCT
    val pk = "product_id";
    val tableProduct = QRealTimeConstant.MYDQL_DIM_PRODUCT

    new DBQuery(tableProduct, schema, pk, sql)
  }

  /**
    * 构造酒店数据查询对象
    */
  def createPubDBQuery():DBQuery = {
    val sql = QRealTimeConstant.SQL_PUB
    val schema = QRealTimeConstant.SCHEMA_PUB
    val pk = "pub_id";
    val tablePub = QRealTimeConstant.MYDQL_DIM_PUB

    new DBQuery(tablePub, schema, pk, sql)
  }



  /**
    * 实时开窗聚合数据
    * 多维表处理
    */
  def handleOrdersMWideAsyncJob(appName:String, fromTopic:String, toTopic:String, groupID:String):Unit = {
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

      /**
        * 3 订单数据
        *   原始明细数据转换操作
        */
      val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      val orderDetailDStream :DataStream[OrderDetailData] = dStream.map(new OrderDetailDataMapFun())


      /**
        * 4 设置事件时间提取器及水位计算
        *   固定范围的水位指定(注意时间单位)
        */
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      orderDetailDStream.assignTimestampsAndWatermarks(ordersPeriodicAssigner)
      //orderDetailDStream.print("order.orderDStream---")

      /**
        * 5 异步维表数据提取
        *   旅游产品维度数据
        */
      //多维表处理
      val dbPath = QRealTimeConstant.MYSQL_CONFIG_URL
      val productDBQuery :DBQuery = createProductDBQuery()
      val pubDBQuery :DBQuery = createPubDBQuery()
      val dbQuerys: mutable.Map[String,DBQuery] = mutable.Map[String,DBQuery](QRealTimeConstant.MYDQL_DIM_PRODUCT -> productDBQuery, QRealTimeConstant.MYDQL_DIM_PUB -> pubDBQuery)

      val syncMFunc = new DimProductMAsyncFunction(dbPath, dbQuerys)
      val asyncMulDS :DataStream[OrderMWideData] = AsyncDataStream.unorderedWait(orderDetailDStream, syncMFunc, QRealTimeConstant.DYNC_DBCONN_TIMEOUT, TimeUnit.MINUTES, QRealTimeConstant.DYNC_DBCONN_CAPACITY)
      asyncMulDS.print("asyncMulDS===>")

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersWideAsyncHander.err:" + ex.getMessage)
      }
    }

  }



  /**
    * 实时开窗聚合数据
    */
  def handleOrdersWideAsyncJob(appName:String, fromTopic:String, toTopic:String, groupID:String):Unit = {

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


      /**
        * 3 订单数据
        *   原始明细数据转换操作
        */
      val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      val orderDetailDStream :DataStream[OrderDetailData] = dStream.map(new OrderDetailDataMapFun())


      /**
        * 4 设置事件时间提取器及水位计算
        *   固定范围的水位指定(注意时间单位)
        */
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      orderDetailDStream.assignTimestampsAndWatermarks(ordersPeriodicAssigner)
      //orderDetailDStream.print("order.orderDStream---")

      /**
        * 5 异步维表数据提取
        *   旅游产品维度数据
        */
      //单维表处理
      val useLocalCache :Boolean = false
      val dbPath = QRealTimeConstant.MYSQL_CONFIG_URL
      val productDBQuery :DBQuery = createProductDBQuery()
      val syncFunc = new DimProductAsyncFunction(dbPath, productDBQuery, useLocalCache)
      val asyncDS :DataStream[OrderWideData] = AsyncDataStream.unorderedWait(orderDetailDStream, syncFunc, QRealTimeConstant.DYNC_DBCONN_TIMEOUT, TimeUnit.MINUTES, QRealTimeConstant.DYNC_DBCONN_CAPACITY)
      asyncDS.print("asyncDS===>")


      /**
        * 6 订单宽表数据打回kafka为下游环节计算准备数据
        */
      val kafkaSerSchema = new OrderWideKSchema(toTopic)
      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val travelKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        kafkaSerSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

      // 加入kafka摄入时间
      travelKafkaProducer.setWriteTimestampToKafka(true)
      asyncDS.addSink(travelKafkaProducer)


      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersWideAsyncHander.err:" + ex.getMessage)
      }
    }

  }


  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)

    val appName = "flink.OrdersWideAsyncHander"
    val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val toTopic = QRealTimeConstant.TOPIC_ORDER_DW_WIDE
    val groupID = "group.OrdersWideAsyncHander"

    //1 维表数据异步处理形成宽表
    handleOrdersWideAsyncJob(appName, fromTopic, toTopic, groupID)

    //2 多维表数据异步处理形成宽表
    handleOrdersMWideAsyncJob(appName, fromTopic, toTopic, groupID)



  }

}
