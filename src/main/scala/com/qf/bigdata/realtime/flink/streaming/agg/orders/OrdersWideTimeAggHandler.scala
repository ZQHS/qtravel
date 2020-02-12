package com.qf.bigdata.realtime.flink.streaming.agg.orders

import java.util.Properties

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.OrderWideGroupKSchema
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun._
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersAggFun.{OrderWideTimeAggFun, OrderWideTimeWindowFun}
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDimDO.ProductDimDO
import com.qf.bigdata.realtime.flink.streaming.rdo.typeinformation.QRealTimeDimTypeInformations
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.slf4j.{Logger, LoggerFactory}


/**
  * 旅游订单业务实时计算
  */
object OrdersWideTimeAggHandler {


  val logger :Logger = LoggerFactory.getLogger("OrdersWideTimeAggHandler")



  /**
    * 实时开窗聚合数据
    */
  def handleOrdersWideAggWindowJob(appName:String, fromTopic:String, toTopic:String, groupID:String):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
      env.getConfig.setAutoWatermarkInterval(QRealTimeConstant.FLINK_WATERMARK_INTERVAL)

      /**
        * 2 离线维度数据提取
        *   旅游产品维度数据
        */
      val productDimFieldTypes :List[TypeInformation[_]] = QRealTimeDimTypeInformations.getProductDimFieldTypeInfos()
      val sql = QRealTimeConstant.SQL_PRODUCT
      val productDS :DataStream[ProductDimDO] = FlinkHelper.createOffLineDataStream(env, sql, productDimFieldTypes).map(
        row => {
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
      kafkaConsumer.setCommitOffsetsOnCheckpoints(true)

      /**
        * 4 订单数据
        *   原始明细数据转换操作
        */
      val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      val orderDStream :DataStream[OrderDetailData] = dStream.map(new OrderDetailDataMapFun())
      //orderDStream.print("orderDStream---:")

      //状态描述对象
      val productMSDesc = new MapStateDescriptor[String, ProductDimDO](QRealTimeConstant.BC_PRODUCT, createTypeInformation[String], createTypeInformation[ProductDimDO])
      val dimProductBCStream :BroadcastStream[ProductDimDO] = productDS.broadcast(productMSDesc)

      /**
        * 5 旅游产品宽表数据
        * 1 产品维度
        * 2 订单数据
        */
      val orderWideGroupDStream :DataStream[OrderWideData] = orderDStream.connect(dimProductBCStream)
        .process(new OrderWideBCFunction(QRealTimeConstant.BC_PRODUCT))
      //orderWideGroupDStream.print("order.orderWideGroupDStream---")



      /**
        * 6 开窗聚合操作
        */
      val aggDStream:DataStream[OrderWideTimeAggDimMeaData] = orderWideGroupDStream.keyBy({
        (wide:OrderWideData) => OrderWideAggDimData(wide.productType, wide.toursimType)
      })
        .window(TumblingProcessingTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE)))
        .aggregate(new OrderWideTimeAggFun(), new OrderWideTimeWindowFun())
      aggDStream.print("order.aggDStream---:")


      /**
        * 7 数据输出Kafka
        */
      val orderWideGroupKSerSchema = new OrderWideGroupKSchema(toTopic)
      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val travelKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        orderWideGroupKSerSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

      //加入kafka摄入时间
      travelKafkaProducer.setWriteTimestampToKafka(true)
      aggDStream.addSink(travelKafkaProducer)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersWideTimeAggHandler.err:" + ex.getMessage)
      }
    }

  }



  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)

    val appName = "qf.OrdersWideTimeAggHandler"
    val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS

    val toTopic = QRealTimeConstant.TOPIC_ORDER_MID
    val groupID = "group.OrdersWideTimeAggHandler"

    //实时处理第二层：宽表处理
    handleOrdersWideAggWindowJob(appName, fromTopic, toTopic, groupID)


  }

}
