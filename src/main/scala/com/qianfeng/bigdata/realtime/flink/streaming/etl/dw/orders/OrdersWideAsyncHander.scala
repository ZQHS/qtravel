package com.qianfeng.bigdata.realtime.flink.streaming.etl.dw.orders

import java.util.concurrent.TimeUnit

import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qianfeng.bigdata.realtime.flink.streaming.assinger.OrdersPeriodicAssigner
import com.qianfeng.bigdata.realtime.flink.streaming.schema.OrderWideKSchema
import com.qianfeng.bigdata.realtime.flink.streaming.funs.common.QRealtimeCommFun._
import com.qianfeng.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun._
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{OrderDetailData, OrderDetailSimData, OrderMWideData, OrderWideData}
import com.qianfeng.bigdata.realtime.flink.util.help.FlinkHelper
import com.qianfeng.bigdata.realtime.util.PropertyUtil
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
    //查询sql
    val sql = QRealTimeConstant.SQL_PRODUCT
    //查询数据集对应schema
    val schema = QRealTimeConstant.SCHEMA_PRODUCT
    //主键
    val pk = QRealTimeConstant.MYSQL_FIELD_PRODUCT_ID;
    //查询目标表
    val tableProduct = QRealTimeConstant.MYDQL_DIM_PRODUCT

    DBQuery(tableProduct, schema, pk, sql)
  }

  /**
    * 构造酒店数据查询对象
    */
  def createPubDBQuery():DBQuery = {
    //查询sql
    val sql = QRealTimeConstant.SQL_PUB
    //查询数据集对应schema
    val schema = QRealTimeConstant.SCHEMA_PUB
    //主键
    val pk = QRealTimeConstant.MYSQL_FIELD_PUB_ID;
    //查询目标表
    val tablePub = QRealTimeConstant.MYDQL_DIM_PUB

    new DBQuery(tablePub, schema, pk, sql)
  }



  /**
    * 旅游产品订单数据实时开窗聚合
    * 多维表处理:旅游产品维表+酒店维表
    */
  def handleOrdersMWideAsyncJob(appName:String, groupID:String, fromTopic:String):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      //注意：检查点时间间隔单位：毫秒
      val checkpointInterval = QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL
      val tc = TimeCharacteristic.EventTime
      val watermarkInterval= QRealTimeConstant.FLINK_WATERMARK_INTERVAL
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(checkpointInterval, tc, watermarkInterval)


      /**
        * 2 kafka流式数据源
        *   kafka消费配置参数
        *   kafka消费策略
        */
      val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, groupID)


      /**
        * 3 旅游产品订单数据
        *   (1) kafka数据源(原始明细数据)->转换操作
        *   (2) 设置执行任务并行度
        *   (3) 设置水位及事件时间(如果时间语义为事件时间)
        */
      //固定范围的水位指定(注意时间单位)
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      val orderDetailDStream :DataStream[OrderDetailData] = env.addSource(kafkaConsumer)
                                           .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
                                           .map(new OrderDetailDataMapFun())
                                           .assignTimestampsAndWatermarks(ordersPeriodicAssigner)


      /**
        * 4 异步维表数据提取
        *   多维表：旅游产品维表+酒店维表
        */
      val dbPath = QRealTimeConstant.MYSQL_CONFIG_URL
      val productDBQuery :DBQuery = createProductDBQuery()
      val pubDBQuery :DBQuery = createPubDBQuery()
      val dbQuerys: mutable.Map[String,DBQuery] = mutable.Map[String,DBQuery](QRealTimeConstant.MYDQL_DIM_PRODUCT -> productDBQuery, QRealTimeConstant.MYDQL_DIM_PUB -> pubDBQuery)

      //异步IO操作
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
  def handleOrdersWideAsyncJob(appName:String, groupID:String, fromTopic:String, toTopic:String):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      //注意：检查点时间间隔单位：毫秒
      val checkpointInterval = QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL
      val tc = TimeCharacteristic.EventTime
      val watermarkInterval= QRealTimeConstant.FLINK_WATERMARK_INTERVAL
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(
        checkpointInterval, tc, watermarkInterval)


      /**
        * 2 kafka流式数据源
        *   kafka消费配置参数
        *   kafka消费策略
        */
      val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, groupID)


      /**
        * 3 旅游产品订单数据
        *   (1) kafka数据源(原始明细数据)->转换操作
        *   (2) 设置执行任务并行度
        *   (3) 设置水位及事件时间(如果时间语义为事件时间)
        */
      //固定范围的水位指定(注意时间单位)
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      val orderDetailDStream :DataStream[OrderDetailData] = env.addSource(kafkaConsumer)
        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
        .map(new OrderDetailDataMapFun())
        .assignTimestampsAndWatermarks(ordersPeriodicAssigner)
      //orderDetailDStream.print("order.orderDStream---")

      /**
        * 4 异步维表数据提取
        *   单维表：旅游产品维表数据
        */
      val useLocalCache :Boolean = false
      val dbPath = QRealTimeConstant.MYSQL_CONFIG_URL
      // 封装查询字段信息
      val productDBQuery :DBQuery = createProductDBQuery()
      // 将流数据和mysql维表数据关联生成对应宽表
      val syncFunc = new DimProductAsyncFunction(dbPath, productDBQuery, useLocalCache)
      // 将当前的宽表处理放入异步操作算子中，那么此时这个异步不会遵循水位线和窗口执行触发要求，而且按照自己的异步时间执行
      val asyncDS :DataStream[OrderWideData] = AsyncDataStream.unorderedWait(
        orderDetailDStream,
        syncFunc,
        QRealTimeConstant.DYNC_DBCONN_TIMEOUT,
        TimeUnit.MINUTES,
        QRealTimeConstant.DYNC_DBCONN_CAPACITY
      )
      asyncDS.print("asyncDS===>")


      /**
        * 5 数据流(如DataStream[OrderWideData])输出sink(如kafka、es等)
        *   (1) kafka数据序列化处理 如OrderWideKSchema
        *   (2) kafka生产者语义：AT_LEAST_ONCE 至少一次
        *   (3) 设置kafka数据加入摄入时间 setWriteTimestampToKafka
        */
      val kafkaSerSchema = new OrderWideKSchema(toTopic)
      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val travelKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        kafkaSerSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
      // 记录Kafka写入的时间
      travelKafkaProducer.setWriteTimestampToKafka(true)
      // 写入kafka的Topic中
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
    //应用程序名称
    val appName = "flink.OrdersWideAsyncHander"

    //kafka消费组
    val groupID = "group.OrdersWideAsyncHander"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val fromTopic = "t_travel_ods"

    //kafka数据输出topic
    //val toTopic = QRealTimeConstant.TOPIC_ORDER_DW_WIDE
    val toTopic = "test_dw"

    //1 维表数据异步处理形成宽表
    handleOrdersWideAsyncJob(appName, groupID, fromTopic, toTopic)

    //2 多维表数据异步处理形成宽表
    handleOrdersMWideAsyncJob(appName, groupID, fromTopic)



  }

}
