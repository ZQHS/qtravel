package com.qf.bigdata.realtime.flink.streaming.etl.ods

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.qf.bigdata.realtime.enumes.ActionEnum
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.UserLogsKSchema
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogData, UserLogLaunchData}
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

/**
  * 实时数据采集
  * 用户启动日志数据
  */
object UserLogsLaunchBatchRec {

  val logger :Logger = LoggerFactory.getLogger("UserLogsLaunchBatchRec")


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
  def handleLaunchRowHDFSJob(appName:String, fromTopic:String, output:String, rolloverInterval:Long, inactivityInterval:Long, maxSize:Long, bucketCheckInterval:Long):Unit = {

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
      //数据过滤
      val launchDStream :DataStream[UserLogLaunchData] = dStream.filter(_.action.equalsIgnoreCase(ActionEnum.LAUNCH.getCode))
        .map(
          (data : UserLogData) => {
            UserLogLaunchData(data.userDevice, data.userID, data.userRegion, data.userRegionIP, data.networkType, data.ct)
          }
        )
      val launchRowDStream :DataStream[String] = launchDStream.map(
        JsonUtil.gObject2Json(_)
      )

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

      launchRowDStream.addSink(hdfsSink)
      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("UserLogsLaunchBatchRec.err:" + ex.getMessage)
      }
    }

  }

  /**
    * 实时统计
    */
  def handleLaunchBatchHDFSJob(appName:String, fromTopic:String, output:String, rolloverInterval:Long, inactivityInterval:Long, maxSize:Long, bucketCheckInterval:Long):Unit = {
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
      //数据过滤
      val launchDStream :DataStream[UserLogLaunchData] = dStream.filter(_.action.equalsIgnoreCase(ActionEnum.LAUNCH.getCode))
        .map(
          (data : UserLogData) => {
            UserLogLaunchData(data.userDevice, data.userID, data.userRegion, data.userRegionIP, data.networkType, data.ct)
          }
        )

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


      //数据分桶分配器
      val bucketAssigner :BucketAssigner[UserLogLaunchData,String] = new DateTimeBucketAssigner(QRealTimeConstant.FORMATTER_YYYYMMDDHH)


      //4 数据实时采集落地
      val launchSchema: Schema = QParquetHelper.generateSchema4File(QParquetHelper.USER_LOGS_LAUNCH)
      val hdfsSink: StreamingFileSink[UserLogLaunchData] = StreamingFileSink.forBulkFormat(outputPath, ParquetAvroWriters.forReflectRecord(classOf[UserLogLaunchData]))
        .withBucketAssigner(bucketAssigner)
        .withBucketCheckInterval(bucketCheckInl)
        .build();

      launchDStream.addSink(hdfsSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("UserLogsLaunchBatchRec.err:" + ex.getMessage)
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

  val appName = "qf.UserLogsLaunchBatchRec"
  val fromTopic = QRealTimeConstant.TOPIC_LOG_ODS
  val output = "hdfs://hdfsCluster/travel/realtime/userlogs/launch/"

  val rolloverInterval:Long = QRealTimeConstant.RM_REC_ROLLOVER_INTERVAL
  val inactivityInterval:Long = QRealTimeConstant.RM_REC_INACTIVITY_INTERVAL
  val maxSize:Long = QRealTimeConstant.RM_REC_MAXSIZE
  val bucketCheckInterval:Long = QRealTimeConstant.RM_REC_BUCKET_CHECK_INTERVAL

  //实时处理第一层：ETL
  handleLaunchRowHDFSJob(appName, fromTopic, output, rolloverInterval, inactivityInterval, maxSize, bucketCheckInterval)



}


}
