package com.qf.bigdata.realtime.flink.streaming.etl.ods

import java.util.Properties

import com.qf.bigdata.realtime.enumes.{ActionEnum, EventEnum}
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.{UserLogsViewListFactKSchema, UserLogsViewListKSchema}
import com.qf.bigdata.realtime.flink.streaming.funs.logs.UserLogsETLFun.{UserLogsViewListFlatMapFun}
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogViewListData, UserLogViewListFactData}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.config.StartupMode

/**
  * 用户行为日志 浏览产品列表数据实时ETL
  * 首次处理：进行数据规范、ETL操作
  */
object UserLogsViewListHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("UserLogsViewListHandler")


  /**
    * 用户行为日志(浏览产品列表行为数据 如搜索、滑动后看到的产品、咨询等列表信息)实时明细数据ETL处理
    * @param appName 程序名称
    * @param fromTopic 数据源输入 kafka topic
    * @param groupID 消费组id
    * @param toTopic 输出kafka topic
    */
  def handleLogsETL4KafkaJob(appName:String, groupID:String, fromTopic:String, toTopic:String):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用处理时间)
        */
      //注意：检查点时间间隔单位：毫秒
      val checkpointInterval = QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL
      val tc = TimeCharacteristic.ProcessingTime
      val watermarkInterval= QRealTimeConstant.FLINK_WATERMARK_INTERVAL
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(checkpointInterval, tc, watermarkInterval)


      /**
        * 2 kafka流式数据源
        *   kafka消费配置参数
        *   kafka消费策略
        *   创建flink消费对象FlinkKafkaConsumer
        *   用户行为日志(kafka数据)反序列化处理
        */
      val schema:KafkaDeserializationSchema[UserLogViewListData] = new UserLogsViewListKSchema(fromTopic)
      val kafkaConsumer : FlinkKafkaConsumer[UserLogViewListData] = FlinkHelper.createKafkaSerDeConsumer(env, fromTopic, groupID, schema, StartupMode.LATEST)


      /**
        * 3 创建【产品列表浏览】日志数据流
        *   (1) 基于处理时间的实时计算：ProcessingTime
        *   (2) 数据过滤
        *   (3) 数据转换
        */
      val viewListFactDStream :DataStream[UserLogViewListFactData] = env.addSource(kafkaConsumer)
        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
        .filter(
          (log : UserLogViewListData) => {
            log.action.equalsIgnoreCase(ActionEnum.INTERACTIVE.getCode) && EventEnum.getViewListEvents.contains(log.eventType)
          }
        ).flatMap(new UserLogsViewListFlatMapFun())
      viewListFactDStream.print("=====viewListFactDStream========")


      /**
        * 4 写入下游环节Kafka
        *   (具体下游环节取决于平台的技术方案和相关需求,如flink+druid技术组合)
        */
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
    //应用程序名称
    val appName = "qf.UserLogsViewListHandler"
    //kafka消费组
    val groupID = "qf.UserLogsViewListHandler"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_LOG_ODS
    val fromTopic = "test_logs"

    //ETL后的明细日志数据输出kafka
    //val toTopic = QRealTimeConstant.TOPIC_LOG_ACTION_VIEWLIST
    val toTopic = "test_logs_viewlist"

    //明细数据输出kafka
    handleLogsETL4KafkaJob(appName, groupID, fromTopic, toTopic)

  }

}
