package com.qf.bigdata.realtime.flink.streaming.agg

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.etl.QRealtimeEtl.handleStreamingETLJob
import org.slf4j.{Logger, LoggerFactory}

/**
  * 聚合操作
  */
object QRealtimeAgg {


  val logger :Logger = LoggerFactory.getLogger("QRealtimeAgg")


  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)

    val appName = "qf.bigdata.realtime.sink.QRealtimeDetailESSink"
    val fromTopic = QRealTimeConstant.TOPIC_FROM
    val toTopic = QRealTimeConstant.TOPIC_TO


    //实时处理第一层：ETL



  }


}
