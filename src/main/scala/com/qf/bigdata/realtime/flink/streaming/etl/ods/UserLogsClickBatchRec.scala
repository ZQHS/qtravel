package com.qf.bigdata.realtime.flink.streaming.etl.ods

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.qf.bigdata.realtime.enumes.{ActionEnum, EventEnum}
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.UserLogsKSchema
import com.qf.bigdata.realtime.flink.streaming.funs.logs.UserLogsETLFun.UserLogClickDataMapFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogClickData, UserLogData, UserLogLaunchData}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import com.qf.bigdata.realtime.util.format.QParquetHelper
import com.qf.bigdata.realtime.util.json.JsonUtil
import org.apache.avro.Schema
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.kafka.common.utils.Time

/**
  * 实时数据采集
  * 用户点击日志数据
  */
object UserLogsClickBatchRec {

  val logger :Logger = LoggerFactory.getLogger("UserLogsClickBatchRec")


  /**
    * flink读取kafka数据
    * @param env
    * @param topic
    * @param properties
    * @return
    */
  def createKafkaConsumer(env:StreamExecutionEnvironment, topic:String, properties:Properties) :FlinkKafkaConsumer[UserLogData] = {
    //创建消费者和消费策略
    val schema:KafkaDeserializationSchema[UserLogData] = new UserLogsKSchema(topic)
    val kafkaConsumer : FlinkKafkaConsumer[UserLogData] = new FlinkKafkaConsumer[UserLogData](topic, schema, properties)
    kafkaConsumer
  }



  /**
    * 实时统计
    */
  def handleClickRowHDFSJob(appName:String, fromTopic:String, output:String, rolloverInterval:Long, inactivityInterval:Long, maxSize:Long, bucketCheckInterval:Long):Unit = {
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
      //clickDStream.print("=====clickDStream========")

      val clickRowDStream :DataStream[String] = clickDStream.map(
        JsonUtil.gObject2Json(_)
      )
      clickRowDStream.print("========clickRowDStream==========")

      //4 数据实时采集落地
      //数据落地路径
      val outputPath :Path = new Path(output)
      //落地大小阈值
      val maxPartSize = 1024l * 1024l * maxSize
      //落地时间间隔
      val rolloverInl = TimeUnit.MINUTES.toMillis(rolloverInterval)
      //无数据间隔时间
      val inactivityInl = TimeUnit.MINUTES.toMillis(inactivityInterval)
      //分桶检查点时间间隔
      val bucketCheckInl = TimeUnit.MINUTES.toMillis(bucketCheckInterval)

      //落地策略
      val rollingPolicy :DefaultRollingPolicy[String,String] = DefaultRollingPolicy.create()
        .withRolloverInterval(rolloverInl)
        .withInactivityInterval(inactivityInl)
        .withMaxPartSize(maxPartSize)
        .build()

      //数据分桶分配器
      val bucketAssigner :BucketAssigner[String,String] = new DateTimeBucketAssigner(QRealTimeConstant.FORMATTER_YYYYMMDDHH)

      //输出sink
      val hdfsSink: StreamingFileSink[String] = StreamingFileSink
        .forRowFormat(outputPath, new SimpleStringEncoder[String]("UTF-8"))
        .withBucketAssigner(bucketAssigner)
        .withRollingPolicy(rollingPolicy)
        .withBucketCheckInterval(bucketCheckInl)
        .build()



      clickRowDStream.addSink(hdfsSink)
      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("UserLogsClickBatchRec.err:" + ex.getMessage)
      }
    }

  }

  /**
    * 实时统计
    */
  def handleClickBatchHDFSJob(appName:String, fromTopic:String, output:String, rolloverInterval:Long, inactivityInterval:Long, maxSize:Long, bucketCheckInterval:Long):Unit = {
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
      //clickDStream.print("=====clickDStream========")


      //4 数据实时采集落地
      //数据落地路径
      val outputPath :Path = new Path(output)
//      //落地大小阈值
//      val maxPartSize = 1024l * 1024l * maxSize
//      //落地时间间隔
//      val rolloverInl = TimeUnit.MINUTES.toMillis(rolloverInterval)
//      //无数据间隔时间
//      val inactivityInl = TimeUnit.MINUTES.toMillis(inactivityInterval)
      //分桶检查点时间间隔
      val bucketCheckInl = TimeUnit.MINUTES.toMillis(bucketCheckInterval)


      //数据分桶分配器
      val bucketAssigner :BucketAssigner[UserLogClickData,String] = new DateTimeBucketAssigner(QRealTimeConstant.FORMATTER_YYYYMMDDHH)


      //4 数据实时采集落地
      //val launchSchema: Schema = QParquetHelper.generateSchema4File(QParquetHelper.USER_LOGS_LAUNCH)
      val hdfsSink: StreamingFileSink[UserLogClickData] = StreamingFileSink.forBulkFormat(outputPath, ParquetAvroWriters.forReflectRecord(classOf[UserLogClickData]))
        .withBucketAssigner(bucketAssigner)
        .withBucketCheckInterval(bucketCheckInl)
        .build();

      clickDStream.addSink(hdfsSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("UserLogsClickBatchRec.err:" + ex.getMessage)
      }
    }

  }



def main(args: Array[String]): Unit = {
  //参数处理
  //    val parameterTool = ParameterTool.fromArgs(args)
  //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
  //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
  //    val output = parameterTool.get(QRealTimeConstant.KEY_RM_REC_OUTPUT)
  //    val rolloverInterval = parameterTool.get(QRealTimeConstant.KEY_RM_REC_ROLLOVER_INTERVAL)
  //    val inactivityInterval = parameterTool.get(QRealTimeConstant.KEY_RM_REC_INACTIVITY_INTERVAL)
  //    val maxSize = parameterTool.get(QRealTimeConstant.KEY_RM_REC_MAXSIZE)
  //    val bucketCheckInterval = parameterTool.get(QRealTimeConstant.KEY_RM_REC_BUCKET_CHECK_INTERVAL)

  val appName = "qf.UserLogsClickBatchRec"
  val fromTopic = QRealTimeConstant.TOPIC_LOG_ODS
  val output = "hdfs://node11:9000/travel/userlogs/click/"

  val rolloverInterval:Long = QRealTimeConstant.RM_REC_ROLLOVER_INTERVAL
  val inactivityInterval:Long = QRealTimeConstant.RM_REC_INACTIVITY_INTERVAL
  val maxSize:Long = QRealTimeConstant.RM_REC_MAXSIZE
  //val bucketCheckInterval:Long = QRealTimeConstant.RM_REC_BUCKET_CHECK_INTERVAL

  val bucketCheckInterval:Long = 1

  //实时处理落地
  //handleClickRowHDFSJob(appName, fromTopic, output, rolloverInterval, inactivityInterval, maxSize, bucketCheckInterval)


  //实时处理落地
  val output2 = "hdfs://node11:9000/travel/userlogs/click_bath/"
  //handleClickBatchHDFSJob(appName, fromTopic, output2, rolloverInterval, inactivityInterval, maxSize, bucketCheckInterval)


}


}
