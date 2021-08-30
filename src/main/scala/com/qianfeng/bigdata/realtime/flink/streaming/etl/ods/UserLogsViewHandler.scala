package com.qianfeng.bigdata.realtime.flink.streaming.etl.ods

import com.qianfeng.bigdata.realtime.enumes.ActionEnum
import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qianfeng.bigdata.realtime.flink.streaming.assinger.UserLogsAssigner
import com.qianfeng.bigdata.realtime.flink.streaming.funs.logs.UserLogPageViewDataMapFun
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qianfeng.bigdata.realtime.flink.streaming.schema.{UserLogsKSchema, UserLogsPageViewKSchema}
import com.qianfeng.bigdata.realtime.flink.streaming.sink.logs.UserLogsViewESSink
import com.qianfeng.bigdata.realtime.flink.util.help.FlinkHelper
import com.qianfeng.bigdata.realtime.util.PropertyUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.config.StartupMode
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._

/**
 * 用户行为日志 页面浏览数据实时ETL
 * 首次处理：进行数据规范、ETL操作
 */
object UserLogsViewHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("UserLogsViewHandler")

  /**
   * 用户行为日志(页面浏览行为数据)实时明细数据ETL处理
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
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment()

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
       * 3 设置事件时间提取器及水位计算
       *   方式：自定义实现AssignerWithPeriodicWatermarks 如 UserLogsAssigner
       */
      val userLogsPeriodicAssigner = new UserLogsAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      val viewDStream :DataStream[UserLogPageViewData] = env.addSource(kafkaConsumer)
        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
        .assignTimestampsAndWatermarks(userLogsPeriodicAssigner)
        .filter(
          (log : UserLogData) => {
            (log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_H5.getCode)
              || log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_NATIVE.getCode))
          }
        ).map(new UserLogPageViewDataMapFun())
      viewDStream.print("=====viewDStream========")

      //4 写入下游环节ES(具体下游环节取决于平台的技术方案和相关需求,如flink+es技术组合)
      val viewESSink = new UserLogsViewESSink(indexName)
      viewDStream.addSink(viewESSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("UserLogsViewHandler.err:" + ex.getMessage)
      }
    }
  }

  /**
   * 用户行为日志(页面浏览行为数据)实时明细数据ETL处理
   * @param appName 程序名称
   * @param fromTopic 数据源输入 kafka topic
   * @param groupID 消费组id
   * @param toTopic 输出kafka topic
   */
  def handleLogsETL4KafkaJob(appName:String, groupID:String, fromTopic:String, toTopic:String):Unit = {
    try{
      /**
       * 1 Flink环境初始化
       *   流式处理的时间特征依赖(使用事件时间)
       */
      //注意：检查点时间间隔单位：毫秒

      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment()

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
       * 3 创建页面浏览日志数据流
       *   (1)设置事件时间提取器及水位计算(如果是事件时间)
       *     方式：自定义实现AssignerWithPeriodicWatermarks 如 UserLogsAssigner
       *   (2) 数据过滤
       *   (3) 数据转换
       */
      val userLogsPeriodicAssigner = new UserLogsAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      val viewDStream :DataStream[UserLogPageViewData] = env.addSource(kafkaConsumer)
        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
        .assignTimestampsAndWatermarks(userLogsPeriodicAssigner)
        .filter(
          (log : UserLogData) => {
            // 浏览数据过滤去07 08的行为操作
            (log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_H5.getCode)
              || log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_NATIVE.getCode))
          }
        ).map(new UserLogPageViewDataMapFun())
      viewDStream.print("=====viewDStream========")

      /**
       * 4 写入下游环节Kafka
       *   (具体下游环节取决于平台的技术方案和相关需求,如flink+druid技术组合)
       */
      val kafkaSerSchema :KafkaSerializationSchema[UserLogPageViewData] = new UserLogsPageViewKSchema(toTopic)
      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val viewKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        kafkaSerSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
      //添加时间戳
      viewKafkaProducer.setWriteTimestampToKafka(true)
      //添加sink
      viewDStream.addSink(viewKafkaProducer)
      //触发执行
      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("UserLogsViewHandler.err:" + ex.getMessage)
      }
    }
  }

  //测试
  def main(args: Array[String]): Unit = {
    //应用程序名称
    val appName = "qf.UserLogsViewHandler"
    //kafka消费组
    val groupID = "qf.UserLogsViewHandler"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_LOG_ODS
    val fromTopic = "travel_logs_ods"

    //ETL后的明细日志数据输出kafka
    //val toTopic = QRealTimeConstant.TOPIC_LOG_ACTION_VIEW
    val toTopic = "travel_dw_userPageView_detail"

    //日志数据输出ES(明细搜索或交互式查询)
    val indexName = QRealTimeConstant.ES_INDEX_NAME_LOG_VIEW

    //1 明细数据输出kafka
    //handleLogsETL4KafkaJob(appName, groupID, fromTopic, toTopic)

    //2 明细数据输出es
    handleLogsETL4ESJob(appName, groupID, fromTopic, indexName)
  }
}