package com.qf.bigdata.realtime.flink.streaming.agg.orders

import java.util.{Date, Properties}

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.assigner.OrdersPeriodicAssigner
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersAggFun.{OrderCustomerStatisKeyedProcessFun, OrderStatisWindowProcessFun}
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.{OrderDetailDataMapFun, OrderWideBCFunction}
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDimDO.ProductDimDO
import com.qf.bigdata.realtime.flink.streaming.rdo.typeinformation.QRealTimeDimTypeInformations
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.{CommonUtil, PropertyUtil}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.types.Row

/**
  * 旅游订单业务实时计算
  * 订单数据综合统计
  */
object OrdersCustomerStatisHandler {

  val logger :Logger = LoggerFactory.getLogger("OrdersCustomerStatisHandler")



  /**
    * 实时数据综合统计
    * 基于数据量作为触发条件进行窗口分组聚合
    */
  def handleOrdersWideStatisCustomerJob(appName:String, fromTopic:String, toTopic:String, groupID:String, indexName:String,maxCount :Long, maxInterval :Long):Unit = {

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
      //orderDetailDStream.print(s"order.orderDetailDStream[${CommonUtil.formatDate4Def(new Date())}]---:")

      /**
        * 4 设置事件时间提取器及水位计算
        *   固定范围的水位指定(注意时间单位)
        */
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      orderDetailDStream.assignTimestampsAndWatermarks(ordersPeriodicAssigner)


      /**
        * 5 异步维表数据提取
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

      /**
        * 7综合统计
        */
      val statisDStream:DataStream[OrderWideCustomerStatisData] = orderWideDStream.keyBy(
        (wide:OrderWideData) => {
          OrderWideAggDimData(wide.productType, wide.toursimType)
        }
      )
        .process(new OrderCustomerStatisKeyedProcessFun(maxCount, maxInterval))
      statisDStream.print(s"order.statisDStream[${CommonUtil.formatDate4Def(new Date())}]---:")


      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersCustomerStatisHandler.err:" + ex.getMessage)
      }
    }

  }



  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)
    val appName = "qf.handleOrdersStatisCustomerJob"
    val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val toTopic = QRealTimeConstant.TOPIC_ORDER_DM_STATIS
    val groupID = "group.handleOrdersStatisCustomerJob"
    val indexName = QRealTimeConstant.ES_INDEX_NAME_ORDER_CUSTOMER_STATIS


    //定量触发窗口计算
    val maxCount :Long = 20
    val maxInterval :Long = 5
    handleOrdersWideStatisCustomerJob(appName, fromTopic, toTopic, groupID, indexName, maxCount, maxInterval)



  }


}
