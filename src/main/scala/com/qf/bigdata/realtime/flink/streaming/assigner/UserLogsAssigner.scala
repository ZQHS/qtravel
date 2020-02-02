package com.qf.bigdata.realtime.flink.streaming.assigner

import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogData}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * 用户行为日志事件时间辅助器
  * @param maxOutOfOrderness
  */
class UserLogsAssigner(maxOutOfOrderness :Long) extends AssignerWithPeriodicWatermarks[UserLogData]{

  //当前时间戳
  var currentMaxTimestamp :Long = java.lang.Long.MIN_VALUE



  /**
    * 水印生成
    * @return
    */
  override def getCurrentWatermark: Watermark ={
    var waterMark :Long = java.lang.Long.MIN_VALUE
    if(currentMaxTimestamp != java.lang.Long.MIN_VALUE){
      waterMark = currentMaxTimestamp - maxOutOfOrderness
    }
    new Watermark(waterMark)
  }



  /**
    * 事件时间提取
    * @param element
    * @param previousElementTimestamp
    * @return
    */
  override def extractTimestamp(element: UserLogData, previousElementTimestamp: Long): Long = {
    val ct = element.ct.toInt
    System.out.println("mark:"+element);
    currentMaxTimestamp = Math.max(ct,currentMaxTimestamp)
    println(s"QRealTimeAssigner currentMaxTimestamp=${currentMaxTimestamp}, ct=${ct}")
    ct
  }
}
