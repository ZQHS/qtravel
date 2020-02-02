package com.qf.bigdata.realtime.flink.streaming.etl.ods

import java.util.Properties

import com.qf.bigdata.realtime.enumes.{ActionEnum, EventEnum}
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.{UserLogsViewListFactKSchema, UserLogsViewListKSchema}
import com.qf.bigdata.realtime.flink.streaming.funs.logs.UserLogsETLFun.UserLogsViewListFlatMapFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogViewListData, UserLogViewListFactData}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._

/**
  * 用户行为日志 浏览产品列表数据实时ETL
  * 首次处理：进行数据规范、ETL操作
  */
object UserLogsViewListHandler {

  val logger :Logger = LoggerFactory.getLogger("UserLogsViewListHandler")


  /**
    * 实时明细数据处理
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
      val schema:KafkaDeserializationSchema[UserLogViewListData] = new UserLogsViewListKSchema(fromTopic)
      val kafkaConsumer : FlinkKafkaConsumer[UserLogViewListData] = new FlinkKafkaConsumer[UserLogViewListData](fromTopic, schema, consumerProperties)
      kafkaConsumer.setStartFromLatest()



      //3 实时流数据集合操作
      val dStream :DataStream[UserLogViewListData] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      //产品列表浏览
      val viewListFactDStream :DataStream[UserLogViewListFactData] = dStream.filter(
        (log : UserLogViewListData) => {
          log.action.equalsIgnoreCase(ActionEnum.INTERACTIVE.getCode) && EventEnum.getViewListEvents.contains(log.eventType)
        }
      ).flatMap(new UserLogsViewListFlatMapFun())


      //4 写入下游环节Kafka(具体下游环节取决于平台的技术方案和相关需求,如flink+druid技术组合)
      val kafkaSerSchema :KafkaSerializationSchema[UserLogViewListFactData] = new UserLogsViewListFactKSchema(toTopic)
      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val viewListFactKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        kafkaSerSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
      viewListFactKafkaProducer.setWriteTimestampToKafka(true)

      viewListFactDStream.addSink(viewListFactKafkaProducer)
      viewListFactDStream.print()

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("UserLogsViewListHandler.err:" + ex.getMessage)
      }
    }

  }


  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)

    val appName = "qf.UserLogsViewListHandler"
    val fromTopic = QRealTimeConstant.TOPIC_LOG_ODS
    val toTopic = QRealTimeConstant.TOPIC_LOG_ACTION_VIEWLIST


    //明细数据输出kafka
    handleLogsETL4KafkaJob(appName, fromTopic, toTopic)

  }

}
