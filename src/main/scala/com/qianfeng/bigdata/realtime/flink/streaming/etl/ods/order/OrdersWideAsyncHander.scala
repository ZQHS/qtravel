package com.qianfeng.bigdata.realtime.flink.streaming.etl.ods.order

import java.util.concurrent.TimeUnit

import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qianfeng.bigdata.realtime.flink.streaming.assinger.OrdersPeriodicAssigner
import com.qianfeng.bigdata.realtime.flink.streaming.funs.common.QRealtimeCommFun.{DBQuery, DimProductMAsyncFunction}
import com.qianfeng.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.OrderDetailDataMapFun
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qianfeng.bigdata.realtime.flink.streaming.schema.OrderMWideKSchema
import com.qianfeng.bigdata.realtime.flink.streaming.sink.CommonESSink
import com.qianfeng.bigdata.realtime.flink.util.help.FlinkHelper
import com.qianfeng.bigdata.realtime.util.{JsonUtil, PropertyUtil}
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._

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

    new DBQuery(tableProduct, schema, pk, sql)
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
    val pk = QRealTimeConstant.MYSQL_FIELD_PUB_ID
    //查询目标表
    val tablePub = QRealTimeConstant.MYDQL_DIM_PUB
    new DBQuery(tablePub, schema, pk, sql)
  }

  /**
   * 旅游产品订单数据实时开窗聚合
   * 多维表处理:旅游产品维表+酒店维表
   */
  def handleOrdersMWideAsyncJob(appName:String, groupID:String, fromTopic:String,toTopic:String,indexName:String):Unit = {
    try{
      /**
       * 1 Flink环境初始化
       *   流式处理的时间特征依赖(使用事件时间)
       */
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment()

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
      //产品维度DBQuery
      val productDBQuery :DBQuery = createProductDBQuery()
      //酒店维度的DBQuery
      val pubDBQuery :DBQuery = createPubDBQuery()
      val dbQuerys: mutable.Map[String,DBQuery] = mutable.Map[String,DBQuery](
        QRealTimeConstant.MYDQL_DIM_PRODUCT -> productDBQuery,
        QRealTimeConstant.MYDQL_DIM_PUB -> pubDBQuery)

      //异步IO操作
      val syncMFunc = new DimProductMAsyncFunction(dbPath, dbQuerys)
      val asyncMulDS :DataStream[OrderMWideData] = AsyncDataStream.unorderedWait(
        orderDetailDStream,
        syncMFunc,
        QRealTimeConstant.DYNC_DBCONN_TIMEOUT,
        TimeUnit.MINUTES,
        QRealTimeConstant.DYNC_DBCONN_CAPACITY)
      asyncMulDS.print("asyncMulDS===>")

      /**
       * 5 数据流(如DataStream[OrderWideData])输出sink(如kafka、es等)
       *   (1) kafka数据序列化处理 如OrderWideKSchema
       *   (2) kafka生产者语义：AT_LEAST_ONCE 至少一次
       *   (3) 设置kafka数据加入摄入时间 setWriteTimestampToKafka
       */
      val kafkaSerSchema = new OrderMWideKSchema(toTopic)
      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val travelKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        kafkaSerSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

      travelKafkaProducer.setWriteTimestampToKafka(true)
      asyncMulDS.addSink(travelKafkaProducer)

      /**  同时再打入ES中用于后期搜索
       *   (1) 自定义ESSink输出数据到ES索引
       *   (2) 通用类型ESSink：CommonESSink
       */
      val orderWideDetailESSink = new CommonESSink(indexName)
      asyncMulDS.map(x=>JsonUtil.gObject2Json(x)).print("打入es的宽表的订单数据->")
      asyncMulDS.map(x=>JsonUtil.gObject2Json(x)).addSink(orderWideDetailESSink)

      //触发执行
      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersWideAsyncHander.err:" + ex.getMessage)
      }
    }
  }


  def main(args: Array[String]): Unit = {
    //应用程序名称
    val appName = "flink.OrdersWideAsyncHander"

    //kafka消费组
    val groupID = "group.OrdersWideAsyncHander"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val fromTopic = "travel_ods_orders"

    //kafka数据输出topic
    //val toTopic = QRealTimeConstant.TOPIC_ORDER_DW_WIDE
    val toTopic = "travel_dw_orders_detail"

    // 多维表数据异步处理形成宽表
    handleOrdersMWideAsyncJob(appName, groupID, fromTopic,toTopic,"travel_orders_wide_detail")
  }
}

