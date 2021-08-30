package com.qianfeng.bigdata.realtime.flink.streaming.recdata

import java.util.concurrent.TimeUnit

import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qianfeng.bigdata.realtime.flink.streaming.assinger.OrdersPeriodicAssigner
import com.qianfeng.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.OrderDetailDataMapFun
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.OrderDetailData
import com.qianfeng.bigdata.realtime.flink.util.help.FlinkHelper
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters

/**
 * 订单数据采集落地--
 * 1、落地为hdfs中parquet格式的文件
 */
object OrdersRecHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("OrdersRecHandler")

  /**
   * 旅游订单实时明细数据采集落地(列式文件格式)
   * @param appName 程序名称
   * @param groupID  消费组id
   * @param fromTopic 数据源输入 kafka topic
   * @param output 采集落地路径
   * @param bucketCheckInterval 校验间隔
   */
  def handleParquet2Hdfs(appName:String, groupID:String, fromTopic:String,
                    output:String,bucketCheckInterval:Long):Unit = {
    try{
      /**
       * 1 Flink环境初始化
       *   流式处理的时间特征依赖(使用事件时间)
       */
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment()
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
      //分桶检查点时间间隔
      val bucketCheckInl = TimeUnit.SECONDS.toMillis(bucketCheckInterval)

      //数据分桶分配器 yyyyMMDDHH
      val bucketAssigner :BucketAssigner[OrderDetailData,String] = new DateTimeBucketAssigner(QRealTimeConstant.FORMATTER_YYYYMMDDHH)

      //4 数据实时采集落地
      //需要引入Flink-parquet的依赖
      val hdfsParquetSink: StreamingFileSink[OrderDetailData] = StreamingFileSink.forBulkFormat(outputPath,
        ParquetAvroWriters.forReflectRecord(classOf[OrderDetailData]))
        .withBucketAssigner(bucketAssigner)
        .withBucketCheckInterval(bucketCheckInl)
        .build()
      //将数据流持久化到指定sink
      orderDetailDStream.addSink(hdfsParquetSink)

      //触发执行
      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersRecHandler.err:" + ex.getMessage)
      }
    }

  }

  //测试
  def main(args: Array[String]): Unit = {
    //行数据输出测试
    handleParquet2Hdfs("orderparquet2hdfs",
      "logs-group-id3",
      "travel_ods_orders",
      "hdfs://hadoop01:9000/travel/orders_detail/",
      60)
  }
}
