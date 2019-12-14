package com.qf.bigdata.realtime.flink.streaming.detail

import java.net.InetSocketAddress
import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.functions.QRealTimeDO.{UserLogAggDo, UserLogDWDo}
import com.qf.bigdata.realtime.flink.streaming.detail.QRealtimeDetailFun.{travelDetailMapFun, travelDetailPojoMapFun, travelDetailSimpleMapFun}
import com.qf.bigdata.realtime.flink.streaming.sink.AccumulateESSink
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import com.qf.bigdata.releasetime.flink.streaming.sink.CommonESSink
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._

/**
  * 实时明细数据写入ES
  */
object QRealtimeDetail2ES {

  val logger :Logger = LoggerFactory.getLogger("QRealtimeDetailESSink")

  //json解析
  val objectMapper: ObjectMapper = new ObjectMapper()


  /**
    * 业务处理：数据扁平化处理
    * @param data
    */
  def travelDetailMap(dStream :DataStream[String]) :DataStream[java.util.Map[String,Object]] = {
    val mDataStream :DataStream[java.util.Map[String,Object]]  = dStream.map(new travelDetailMapFun())
    mDataStream
  }

  def travelDetailSimpleMap(dStream :DataStream[String]) :DataStream[UserLogAggDo] = {
    val mDataStream :DataStream[UserLogAggDo]  = dStream.map(new travelDetailSimpleMapFun())
    mDataStream
  }

  def travelDetailPojoMap(dStream :DataStream[String]) :DataStream[UserLogDWDo] = {
    val mDataStream :DataStream[UserLogDWDo]  = dStream.map(new travelDetailPojoMapFun())
    mDataStream
  }



  /**
    * 实时明细数据
    */
  def handleStreamingDetail2ESJob(appName:String, fromTopic:String, groupID:String, indexName:String):Unit = {

    //1 flink环境初始化
    val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
    //使用事件时间做处理参考
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2 kafka流式数据源
    val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    consumerProperties.setProperty("group.id", groupID)

    val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, consumerProperties)
    kafkaConsumer.setStartFromLatest()

    //3 实时流数据集合操作
    val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)

    //4 输出到ES
    val commESSink = new CommonESSink(indexName)
    dStream.addSink(commESSink)

    env.execute(appName)
  }


  /**
    * 实时累计数据
    */
  def handleStreamingAgg2ESJob(appName:String, fromTopic:String, groupID:String, indexName:String):Unit = {

    //1 flink环境初始化
    val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
    //使用事件时间做处理参考
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2 kafka流式数据源
    val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    consumerProperties.setProperty("group.id", groupID)

    val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, consumerProperties)
    kafkaConsumer.setStartFromLatest()

    //3 实时流数据集合操作
    val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
    val mStream :DataStream[java.util.Map[String,Object]] = travelDetailMap(dStream)

    //
    val pSteam :DataStream[UserLogDWDo] = travelDetailPojoMap(dStream)

    //4 输出到ES
    val accumulateESSink = new AccumulateESSink(indexName)
    mStream.addSink(accumulateESSink)

    env.execute(appName)
  }


  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    val appName = "qf.bigdata.realtime.sink.QRealtimeDetail2ES"
    val groupID = "group.QRealtimeDetail2ES"
    val fromTopic = QRealTimeConstant.TOPIC_TO
    val index = QRealTimeConstant.ES_INDEX_DW


    //明细数据处理
    //handleStreamingDetail2ESJob(appName, fromTopic, groupID, QRealTimeConstant.ES_INDEX_DW)

    handleStreamingAgg2ESJob(appName, fromTopic, groupID, QRealTimeConstant.ES_INDEX_DM)

  }


}
