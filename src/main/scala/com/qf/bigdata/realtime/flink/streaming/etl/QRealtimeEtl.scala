package com.qf.bigdata.realtime.flink.streaming.etl

import java.util.Properties

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.schema.QkafkaSerializationSchema
import com.qf.bigdata.realtime.flink.streaming.etl.QRealtimeEtlFun.{travelDataFlatMapFun, travelDataMapFun, travelWideDataMapFun}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic

import scala.collection.mutable

/**
  * 实时数据ETL
  * 首次处理：进行数据规范、ETL操作为下游开窗聚合、
  */
object QRealtimeEtl {


  val logger :Logger = LoggerFactory.getLogger("QRealtimeEtl")

  /**
    * json解析
    */
  val objectMapper: ObjectMapper = new ObjectMapper()


  /**
    * 业务处理：数据扁平化处理
    * @param data
    */
  def travelDataMap(dStream :DataStream[String]) :DataStream[String] = {
    val mDataStream :DataStream[String]  = dStream.map(new travelDataMapFun())
    mDataStream
  }


  /**
    * 业务处理：数据转换
    * @param data
    */
  def travelDataFlatMap(dStream :DataStream[String]) :DataStream[String] = {
    val mDataStream :DataStream[String]  = dStream.flatMap(new travelDataFlatMapFun())
    mDataStream
  }


  /**
    * 实时统计
    */
  def handleStreamingETLJob(appName:String, fromTopic:String, toTopic:String):Unit = {

    //1 flink环境初始化
    val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
    //使用事件时间做处理参考
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2 kafka流式数据源
    val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, consumerProperties)
    kafkaConsumer.setStartFromLatest()

    //3 实时流数据集合操作
    val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
    val mStream :DataStream[String] = travelDataMap(dStream)
    val fmStream:DataStream[String] = travelDataFlatMap(mStream)

    //5 写入下游环节
    val kafkaSerSchema = new QkafkaSerializationSchema(toTopic)
    val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
    val travelKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        kafkaSerSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

    //6 加入kafka摄入时间
    travelKafkaProducer.setWriteTimestampToKafka(true)
    fmStream.addSink(travelKafkaProducer)
    fmStream.print()

    env.execute(appName)
  }



  def main(args: Array[String]): Unit = {
      //参数处理
//    val parameterTool = ParameterTool.fromArgs(args)
//    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
//    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
//    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)

    val appName = "com.qf.bigdata.releasetime.flink.streaming.sink.ReleaseRealtimeAgg"
    val fromTopic = QRealTimeConstant.TOPIC_FROM
    val toTopic = QRealTimeConstant.TOPIC_TO


    //实时处理第一层：ETL
    handleStreamingETLJob(appName, fromTopic, toTopic)



  }



}
