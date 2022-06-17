package com.qianfeng.bigdata.realtime.flink.streaming.funs.cep

import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.util.Collector

import java.util
import scala.collection.JavaConversions._
/**
 * 用户行为日志涉及的复杂事件处理
 */
object UserLogsCepFun {

  /**
   * 页面浏览模式匹配处理函数
   */
  class UserLogsViewPatternProcessFun extends PatternProcessFunction[UserLogPageViewData,UserLogPageViewAlertData]{
    override def processMatch(`match`: util.Map[String, util.List[UserLogPageViewData]],
                              ctx: PatternProcessFunction.Context,
                              out: Collector[UserLogPageViewAlertData]): Unit = {
      val datas :util.List[UserLogPageViewData] = `match`.getOrDefault(QRealTimeConstant.FLINK_CEP_VIEW_BEGIN, new util.ArrayList[UserLogPageViewData]())
      if(!datas.isEmpty){
        //需要引入scala和java互转
        for(data <- datas.iterator()){
          // 将数据进行处理，重新封装
          val viewAlertData = UserLogPageViewAlertData(data.userDevice, data.userID, data.userRegion,
            data.userRegionIP, data.duration, data.ct)
          out.collect(viewAlertData)
        }
      }
    }
  }
}

