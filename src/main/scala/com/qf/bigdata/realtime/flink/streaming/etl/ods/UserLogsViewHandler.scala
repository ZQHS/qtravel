package com.qf.bigdata.realtime.flink.streaming.etl.ods

import java.util.Properties

import com.qf.bigdata.realtime.enumes.ActionEnum
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.{UserLogsKSchema, UserLogsPageViewKSchema}
import com.qf.bigdata.realtime.flink.streaming.etl.ods.UserLogsClickHandler.logger
import com.qf.bigdata.realtime.flink.streaming.funs.logs.UserLogsETLFun.UserLogPageViewDataMapFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogData, UserLogPageViewAggMeanData, UserLogPageViewData, UserLogPageViewLowDurationAggMeanData}
import com.qf.bigdata.realtime.flink.streaming.sink.logs.UserLogsViewESSink
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.slf4j.{Logger, LoggerFactory}

/**
  * 用户行为日志 页面浏览数据实时ETL
  * 首次处理：进行数据规范、ETL操作
  */
object UserLogsViewHandler {

  val logger :Logger = LoggerFactory.getLogger("UserLogsViewHandler")




  /**
    * 实时统计
    */
  def handleLogsETL4ESJob(appName:String, fromTopic:String, indexName:String):Unit = {

    try{
      //1 flink环境初始化使用事件时间做处理参考
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.getConfig.setAutoWatermarkInterval(QRealTimeConstant.FLINK_WATERMARK_INTERVAL)

      //2 kafka流式数据源
      val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)

      //创建消费者和消费策略
      val schema:KafkaDeserializationSchema[UserLogData] = new UserLogsKSchema(fromTopic)
      val kafkaConsumer : FlinkKafkaConsumer[UserLogData] = new FlinkKafkaConsumer[UserLogData](fromTopic, schema, consumerProperties)
      kafkaConsumer.setStartFromLatest()

      //3 实时流数据集合操作
      val dStream :DataStream[UserLogData] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      //页面浏览
      val viewDStream :DataStream[UserLogPageViewData] = dStream.filter(
        (log : UserLogData) => {
          log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_H5.getCode) || log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_NATIVE.getCode)
        }
      ).map(new UserLogPageViewDataMapFun())
      viewDStream.print("=======viewDStream==========")


      //4 写入下游环节ES(具体下游环节取决于平台的技术方案和相关需求,如flink+es技术组合)
//      val viewESSink = new UserLogsViewESSink(indexName)
//      viewDStream.addSink(viewESSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("UserLogsViewHandler.err:" + ex.getMessage)
      }
    }

  }


  /**
    * 实时明细处理
    */
  def handleLogsETL4KafkaJob(appName:String, fromTopic:String, toTopic:String):Unit = {

    try{
      //1 flink环境初始化使用事件时间做处理参考
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.getConfig.setAutoWatermarkInterval(QRealTimeConstant.FLINK_WATERMARK_INTERVAL)

      //2 kafka流式数据源
      val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)

      //创建消费者和消费策略
      val schema:KafkaDeserializationSchema[UserLogData] = new UserLogsKSchema(fromTopic)
      val kafkaConsumer : FlinkKafkaConsumer[UserLogData] = new FlinkKafkaConsumer[UserLogData](fromTopic, schema, consumerProperties)
      kafkaConsumer.setStartFromLatest()

      //3 实时流数据集合操作
      val dStream :DataStream[UserLogData] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      //页面浏览
      val viewDStream :DataStream[UserLogPageViewData] = dStream.filter(
        (log : UserLogData) => {
          log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_H5.getCode) || log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_NATIVE.getCode)
        }
      ).map(new UserLogPageViewDataMapFun())


      //4 写入下游环节Kafka(具体下游环节取决于平台的技术方案和相关需求,如flink+druid技术组合)
      val kafkaSerSchema :KafkaSerializationSchema[UserLogPageViewData] = new UserLogsPageViewKSchema(toTopic)
      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val viewKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        kafkaSerSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
      viewKafkaProducer.setWriteTimestampToKafka(true)

      viewDStream.addSink(viewKafkaProducer)
      viewDStream.print()

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("UserLogsViewHandler.err:" + ex.getMessage)
      }
    }

  }


  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)

    val appName = "qf.UserLogsViewHandler"
    val fromTopic = QRealTimeConstant.TOPIC_LOG_ODS
    val toTopic = QRealTimeConstant.TOPIC_LOG_ACTION_VIEW

    val indexName = QRealTimeConstant.ES_INDEX_NAME_LOG_VIEW

    //1 明细数据输出kafka
    //handleLogsETL4KafkaJob(appName, fromTopic, toTopic)

    //2 明细数据输出es
    //handleLogsETL4ESJob(appName, fromTopic, indexName)

  }


}
