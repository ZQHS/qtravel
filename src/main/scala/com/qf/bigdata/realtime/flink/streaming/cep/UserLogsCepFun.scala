package com.qf.bigdata.realtime.flink.streaming.cep

import java.util
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogPageViewAlertData, UserLogPageViewData}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._

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
           for(data <- datas.iterator()){
             val viewAlertData = UserLogPageViewAlertData(data.userDevice, data.userID, data.userRegion,
               data.userRegionIP, data.duration, data.ct)
               out.collect(viewAlertData)
           }
        }
    }
  }


}
