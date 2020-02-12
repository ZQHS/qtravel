package com.qf.bigdata.realtime.flink.streaming.etl.ods

import java.util.Properties

import com.qf.bigdata.realtime.enumes.{ActionEnum, EventEnum}
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.UserLogsKSchema
import com.qf.bigdata.realtime.flink.streaming.funs.logs.UserLogsETLFun.UserLogClickDataMapFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogClickData, UserLogData}
import com.qf.bigdata.realtime.flink.streaming.sink.logs.UserLogsClickESSink
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._

/**
  * 用户行为日志 交互式点击行为数据
  * 首次处理：进行数据规范、ETL操作
  */
object UserLogsClickHandler {

  val logger :Logger = LoggerFactory.getLogger("UserLogsClickHandler")



  /**
    * 实时明细数据ETL处理
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

      //交互点击行为
      val clickDStream :DataStream[UserLogClickData] = dStream.filter(
        (log : UserLogData) => {
          log.action.equalsIgnoreCase(ActionEnum.INTERACTIVE.getCode) && log.eventType.equalsIgnoreCase(EventEnum.CLICK.getCode)
        }
      ).map(new UserLogClickDataMapFun())
      clickDStream.print("=====clickDStream========")


      //4 写入下游环节ES(具体下游环节取决于平台的技术方案和相关需求,如flink+es技术组合)
      val clickESSink = new UserLogsClickESSink(indexName)
      clickDStream.addSink(clickESSink)


      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("UserLogsClickHandler.err:" + ex.getMessage)
      }
    }

  }



  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)

    val appName = "qf.UserLogsClickHandler"
    val fromTopic = QRealTimeConstant.TOPIC_LOG_ODS

    val indexName = QRealTimeConstant.ES_INDEX_NAME_LOG_CLICK

    //明细数据输出es
    handleLogsETL4ESJob(appName, fromTopic, indexName)

  }


}
