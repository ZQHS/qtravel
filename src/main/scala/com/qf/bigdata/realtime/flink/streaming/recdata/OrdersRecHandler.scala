package com.qf.bigdata.realtime.flink.streaming.recdata

import java.util.concurrent.TimeUnit
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.assigner.{OrdersPeriodicAssigner}
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.OrderDetailDataMapFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{OrderDetailData}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.json.JsonUtil
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._

/**
  * 订单数据采集落地
  */
object OrdersRecHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("OrdersRecHandler")


  /**
    * 旅游订单实时明细数据采集落地(行式文件格式)
    * @param appName 程序名称
    * @param groupID  消费组id
    * @param fromTopic 数据源输入 kafka topic
    * @param output 采集落地路径
    * @param rolloverInterval 落地间隔
    * @param inactivityInterval 非交互间隔
    * @param maxSize 数据量
    * @param bucketCheckInterval 校验间隔
    */
  def handleRowHDFSJob(appName:String, groupID:String, fromTopic:String, output:String, rolloverInterval:Long, inactivityInterval:Long, maxSize:Long, bucketCheckInterval:Long):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
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
        */
      val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, groupID)

      /**
        * 4 旅游产品订单数据
        *   (1) json字符串形式->行式记录落地
        */
      val orderDetailDStream :DataStream[String] = env.addSource(kafkaConsumer)
        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
        .map(JsonUtil.object2json(_))
      //orderDetailDStream.print("========orderDetailDStream==========")

      //4 数据实时采集落地
      //数据落地路径
      val outputPath :Path = new Path(output)
      //落地大小阈值
      val maxPartSize = 1024l *  maxSize
      //落地时间间隔
      val rolloverInl = TimeUnit.SECONDS.toMillis(rolloverInterval)
      //无数据间隔时间
      val inactivityInl = TimeUnit.SECONDS.toMillis(inactivityInterval)
      //分桶检查点时间间隔
      val bucketCheckInl = TimeUnit.SECONDS.toMillis(bucketCheckInterval)

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

      orderDetailDStream.addSink(hdfsSink)
      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersRecHandler.row.err:" + ex.getMessage)
      }
    }

  }

  /**
    * 旅游订单实时明细数据采集落地(列式文件格式)
    * @param appName 程序名称
    * @param groupID  消费组id
    * @param fromTopic 数据源输入 kafka topic
    * @param output 采集落地路径
    * @param rolloverInterval 落地间隔
    * @param inactivityInterval 非交互间隔
    * @param maxSize 数据量
    * @param bucketCheckInterval 校验间隔
    */
  def handleHDFSJob(appName:String, groupID:String, fromTopic:String, output:String, rolloverInterval:Long, inactivityInterval:Long, maxSize:Long, bucketCheckInterval:Long):Unit = {
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
        */
      val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, groupID)

      /**
        * 3 旅游产品订单数据
        *   (1) 设置水位及事件时间提取(如果时间语义为事件时间的话)
        *   (2)原始明细数据转换操作(json->业务对象OrderDetailData)
        */
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      val orderDetailDStream :DataStream[OrderDetailData] = env.addSource(kafkaConsumer)
        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
        .map(new OrderDetailDataMapFun())
        .assignTimestampsAndWatermarks(ordersPeriodicAssigner)
      orderDetailDStream.print("order.orderDStream---")


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
      val bucketCheckInl = TimeUnit.SECONDS.toMillis(bucketCheckInterval)

      //数据分桶分配器
      val bucketAssigner :BucketAssigner[OrderDetailData,String] = new DateTimeBucketAssigner(QRealTimeConstant.FORMATTER_YYYYMMDDHH)

      //4 数据实时采集落地
      val hdfsSink: StreamingFileSink[OrderDetailData] = StreamingFileSink.forBulkFormat(outputPath, ParquetAvroWriters.forReflectRecord(classOf[OrderDetailData]))
        .withBucketAssigner(bucketAssigner)
        .withBucketCheckInterval(bucketCheckInl)
        .build();

      orderDetailDStream.addSink(hdfsSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersRecHandler.err:" + ex.getMessage)
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
    //应用程序名称
    val appName = "qf.OrdersRecHandler"

    //kafka消费组
    val groupID = "qf.OrdersRecHandler"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_LOG_ODS
    val fromTopic = "test_ods"

    //落地相关参数
    val rolloverInterval:Long = QRealTimeConstant.RM_REC_ROLLOVER_INTERVAL //落地间隔
    val inactivityInterval:Long = QRealTimeConstant.RM_REC_INACTIVITY_INTERVAL //非交互间隔
    val maxSize:Long = QRealTimeConstant.RM_REC_MAXSIZE //数据量
    val bucketCheckInterval:Long = QRealTimeConstant.RM_REC_BUCKET_CHECK_INTERVAL //校验间隔

    //实时处理落地
    val output = "hdfs://node11:9000/qtravel/orders_row/"
    //handleRowHDFSJob(appName, groupID, fromTopic, output, rolloverInterval, inactivityInterval, maxSize, bucketCheckInterval)


    //实时处理落地
    val output2 = "hdfs://node11:9000/qtravel/orders_column/"
    handleHDFSJob(appName, groupID, fromTopic, output2, rolloverInterval, inactivityInterval, maxSize, bucketCheckInterval)


  }


}
