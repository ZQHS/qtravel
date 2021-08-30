package com.qianfeng.bigdata.realtime.flink.streaming.etl.ods

import com.qianfeng.bigdata.realtime.enumes.{ActionEnum, EventEnum}
import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qianfeng.bigdata.realtime.flink.streaming.funs.logs.UserLogsViewListFlatMapFun
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qianfeng.bigdata.realtime.flink.streaming.schema.{UserLogsViewListFactKSchema, UserLogsViewListKSchema}
import com.qianfeng.bigdata.realtime.flink.util.help.FlinkHelper
import com.qianfeng.bigdata.realtime.util.PropertyUtil
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
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment()

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
            (log.action.equalsIgnoreCase(ActionEnum.INTERACTIVE.getCode)
              && EventEnum.getViewListEvents.contains(log.eventType))
          }
        )
        .flatMap(new UserLogsViewListFlatMapFun())
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
      //添加sink
      viewListFactDStream.addSink(viewListFactKafkaProducer)
      //触发执行
      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("UserLogsViewListHandler.err:" + ex.getMessage)
      }
    }
  }

  //测试
  def main(args: Array[String]): Unit = {
    //应用程序名称
    val appName = "qf.UserLogsViewListHandler"
    //kafka消费组
    val groupID = "qf.UserLogsViewListHandler"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_LOG_ODS
    val fromTopic = "travel_ods_logs"

    //ETL后的明细日志数据输出kafka
    //val toTopic = QRealTimeConstant.TOPIC_LOG_ACTION_VIEWLIST
    val toTopic = "travel_dw_viewList_detail"

    //明细数据输出kafka
    handleLogsETL4KafkaJob(appName, groupID, fromTopic, toTopic)
  }
}
