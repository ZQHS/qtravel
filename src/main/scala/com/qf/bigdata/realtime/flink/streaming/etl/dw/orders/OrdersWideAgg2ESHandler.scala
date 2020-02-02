package com.qf.bigdata.realtime.flink.streaming.etl.dw.orders

import java.util.Properties

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.assigner.OrdersPeriodicAssigner
import com.qf.bigdata.realtime.flink.streaming.etl.ods.UserLogsViewHandler.logger
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.{OrderDetailDataMapFun, OrderWideBCFunction}
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{OrderDetailData, OrderWideData}
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDimDO.ProductDimDO
import com.qf.bigdata.realtime.flink.streaming.rdo.typeinformation.QRealTimeDimTypeInformations
import com.qf.bigdata.realtime.flink.streaming.sink.orders.OrdersWideAggESSink
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}

/**
  * 旅游订单业务实时计算：明细数据
  */
object OrdersWideAgg2ESHandler {


  val logger :Logger = LoggerFactory.getLogger("OrdersWideAgg2ESHandler")



  /**
    * 实时开窗聚合数据
    */
  def handleOrdersWideAccJob(appName:String, fromTopic:String, groupID:String, indexName:String):Unit = {

    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.getConfig.setAutoWatermarkInterval(QRealTimeConstant.FLINK_WATERMARK_INTERVAL)

      /**
        * 2 离线维度数据提取
        *   旅游产品维度数据
        */
      val productDimFieldTypes :List[TypeInformation[_]] = QRealTimeDimTypeInformations.getProductDimFieldTypeInfos()
      //mysql查询sql
      val sql = QRealTimeConstant.SQL_PRODUCT
      val productDS :DataStream[ProductDimDO] = FlinkHelper.createOffLineDataStream(env, sql, productDimFieldTypes).map(
        (row: Row) => {
          val productID = row.getField(0).toString
          val productLevel = row.getField(1).toString.toInt
          val productType = row.getField(2).toString
          val depCode = row.getField(3).toString
          val desCode = row.getField(4).toString
          val toursimType = row.getField(5).toString
          new ProductDimDO(productID, productLevel, productType, depCode, desCode, toursimType)
        }
      )
      //productDS.print("JDBC.DataStream===>")


      /**
        * 3 kafka流式数据源
        *   kafka消费配置参数
        *   kafka消费策略
        */
      val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
      consumerProperties.setProperty("group.id", groupID)

      val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, consumerProperties)
      kafkaConsumer.setStartFromLatest()

      /**
        * 4 订单数据
        *   原始明细数据转换操作
        */
      val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      val orderDetailDStream :DataStream[OrderDetailData] = dStream.map(new OrderDetailDataMapFun())
      //orderDetailDStream.print("orderDStream---:")

      /**
        * 5 设置事件时间提取器及水位计算
        *   固定范围的水位指定(注意时间单位)
        */
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      orderDetailDStream.assignTimestampsAndWatermarks(ordersPeriodicAssigner)
      //orderDStream.print("order.orderDStream---")


      //状态描述对象
      val productMSDesc = new MapStateDescriptor[String, ProductDimDO](QRealTimeConstant.BC_PRODUCT, createTypeInformation[String], createTypeInformation[ProductDimDO])
      val dimProductBCStream :BroadcastStream[ProductDimDO] = productDS.broadcast(productMSDesc)

      /**
        * 6 旅游产品宽表数据
        * 1 产品维度
        * 2 订单数据
        */
      val orderWideDStream :DataStream[OrderWideData] = orderDetailDStream.connect(dimProductBCStream)
        .process(new OrderWideBCFunction(QRealTimeConstant.BC_PRODUCT))
      orderWideDStream.print("order.orderWideDStream---")



      /**
        * 7 数据输出Sink
        *   自定义ESSink输出
        */
      val windowOrderESSink = new OrdersWideAggESSink(indexName)
      orderWideDStream.addSink(windowOrderESSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersWideAgg2ESHandler.err:" + ex.getMessage)
      }
    }

  }


  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)

    val appName = "flink.OrdersWideAgg2ESHandler"
    val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val groupID = "group.OrdersWideAgg2ESHandler"
    val indexName = QRealTimeConstant.ES_INDEX_NAME_ORDER_WIDE_AGG

    handleOrdersWideAccJob(appName, fromTopic, groupID, indexName)


  }

}
