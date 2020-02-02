package com.qf.bigdata.realtime.flink.streaming.cep

import java.util.Properties

import com.qf.bigdata.realtime.enumes.ActionEnum
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.{OrderWideKSchema, UserLogsKSchema}
import com.qf.bigdata.realtime.flink.streaming.assigner.UserLogsAssigner
import com.qf.bigdata.realtime.flink.streaming.funs.logs.UserLogsAggFun.UserLogsLaunchProcessAllWinFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogData, UserLogLaunchData}
import com.qf.bigdata.realtime.flink.streaming.sink.CommonESSink
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import com.qf.bigdata.realtime.util.json.JsonUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.cep.CEP
import org.apache.flink.cep.pattern.Pattern

/**
  * 用户行为日志 启动异常报警处理
  * 基于flink cep
  */
object UserLogsLaunchWarnHandler {

  val logger :Logger = LoggerFactory.getLogger("UserLogsLaunchWarnHandler")


  /**
    * 启动日志报警处理
    * 假设规则：N分钟内启动M次
    */
  def handleLaunchWarnJob(appName:String, fromTopic:String, toTopic:String, timeRange:Long, launchCount:Long):Unit = {

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

    //水位设置(基于事件时间才有)
    val userLogsAssigner = new UserLogsAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
    dStream.assignTimestampsAndWatermarks(userLogsAssigner)

    //数据过滤
    val launchDStream :DataStream[UserLogLaunchData] = dStream.filter(_.action.equalsIgnoreCase(ActionEnum.LAUNCH.getCode))
      .map(
        (data : UserLogData) => {
          UserLogLaunchData(data.userDevice, data.userID, data.userRegion, data.userRegionIP, data.networkType, data.ct)
        }
      )

    //4 设置复杂规则 cep
//    Pattern.begin[UserLogLaunchData](QRealTimeConstant.PATTERN_LAUNCH_USER)
//        .where()
    //CEP.pattern()


    //5 写入下游环节
//    val kafkaSerSchema = new OrderWideKSchema(toTopic)
//    val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
//    val travelKafkaProducer = new FlinkKafkaProducer(
//      toTopic,
//      kafkaSerSchema,
//      kafkaProductConfig,
//      FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
    //
    //    // 加入kafka摄入时间
    //    travelKafkaProducer.setWriteTimestampToKafka(true)
    //    asyncDS.addSink(travelKafkaProducer)

    env.execute(appName)
  }



  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)
    //    val timeRange = parameterTool.get(QRealTimeConstant.KEY_RM_TIME_RANGE)
    //    val launchCount = parameterTool.get(QRealTimeConstant.KEY_RM_LAUNCH_COUNT)

    val appName = "qf.UserLogsLaunchWarnHandler"
    val fromTopic = QRealTimeConstant.TOPIC_LOG_ODS
    val toTopic = QRealTimeConstant.TOPIC_LOG_ACTION_LAUNCH_WARN
    val timeRange = 1l
    val launchCount = 3l


    //启动日志处理
    handleLaunchWarnJob(appName, fromTopic, toTopic, timeRange, launchCount)



  }


}
