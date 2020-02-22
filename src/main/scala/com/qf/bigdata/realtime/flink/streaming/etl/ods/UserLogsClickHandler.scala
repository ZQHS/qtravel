package com.qf.bigdata.realtime.flink.streaming.etl.ods

import com.qf.bigdata.realtime.enumes.{ActionEnum, EventEnum}
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.UserLogsKSchema
import com.qf.bigdata.realtime.flink.streaming.assigner.{UserLogsAssigner}
import com.qf.bigdata.realtime.flink.streaming.funs.logs.UserLogsETLFun.UserLogClickDataMapFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogClickData, UserLogData}
import com.qf.bigdata.realtime.flink.streaming.sink.logs.UserLogsClickESSink
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.config.StartupMode

/**
  * 用户行为日志 交互式点击行为数据
  * 首次处理：进行数据规范、ETL操作
  */
object UserLogsClickHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("UserLogsClickHandler")



  /**
    * 用户行为日志(交互式点击行为数据)实时明细数据ETL处理
    * @param appName 程序名称
    * @param fromTopic 数据源输入 kafka topic
    * @param groupID 消费组id
    * @param indexName 输出ES索引名称
    */
  def handleLogsETL4ESJob(appName:String, groupID:String, fromTopic:String, indexName:String):Unit = {
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
        *   创建flink消费对象FlinkKafkaConsumer
        *   用户行为日志(kafka数据)反序列化处理
        */
      val schema:KafkaDeserializationSchema[UserLogData] = new UserLogsKSchema(fromTopic)
      val kafkaConsumer : FlinkKafkaConsumer[UserLogData] = FlinkHelper.createKafkaSerDeConsumer(env, fromTopic, groupID, schema, StartupMode.LATEST)


      /**
        * 3 创建点击日志数据流
        *   (1)设置事件时间提取器及水位计算(如果是事件时间)
        *     方式：自定义实现AssignerWithPeriodicWatermarks 如 UserLogsAssigner
        *   (2) 数据过滤
        *   (3) 数据转换
        */
      val userLogsPeriodicAssigner = new UserLogsAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      val clickDStream :DataStream[UserLogClickData] = env.addSource(kafkaConsumer)
                                                .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
                                                .assignTimestampsAndWatermarks(userLogsPeriodicAssigner)
                                                .filter(
                                                  (log : UserLogData) => {
                                                    log.action.equalsIgnoreCase(ActionEnum.INTERACTIVE.getCode) && log.eventType.equalsIgnoreCase(EventEnum.CLICK.getCode)
                                                  }
                                                ).map(new UserLogClickDataMapFun())
      clickDStream.print("=====clickDStream========")


      /**
        * 4 点击数据写入ES
        *   (1)自定义ES-Sink
        */
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

    //应用程序名称
    val appName = "flink.UserLogsClickHandler"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_LOG_ODS
    val fromTopic = "test_logs"

    //kafka消费组
    val groupID = "group.UserLogsClickHandler"

    //点击日志数据输出ES(明细搜索或交互式查询)
    val indexName = QRealTimeConstant.ES_INDEX_NAME_LOG_CLICK

    //明细数据输出es
    handleLogsETL4ESJob(appName, groupID, fromTopic, indexName)

  }


}
