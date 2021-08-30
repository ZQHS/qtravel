package com.qianfeng.bigdata.realtime.flink.streaming.funs.logs

import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{UserLogData,UserLogPageViewData}
import com.qianfeng.bigdata.realtime.util.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.MapFunction

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
 * 用户日志页面浏览数据实时数据转换
 */
class UserLogPageViewDataMapFun  extends MapFunction[UserLogData,UserLogPageViewData]{

  override def map(value: UserLogData): UserLogPageViewData = {
    val sid :String = value.sid
    val userDevice:String = value.userDevice
    val userDeviceType:String = value.userDeviceType
    val os:String = value.os
    val userID :String = value.userID
    val userRegion :String = value.userRegion
    val userRegionIP:String = value.userRegionIP
    val lonitude:String = value.lonitude
    val latitude:String = value.latitude
    val manufacturer:String = value.manufacturer
    val carrier:String = value.carrier
    val networkType:String = value.networkType
    val duration:String = value.duration
    val action:String = value.action
    val eventType:String = value.eventType
    val ct:Long = value.ct
    val exts :String = value.exts
    var targetID :String = ""
    if(StringUtils.isNotEmpty(exts)){
      val extMap :mutable.Map[String,AnyRef] = JsonUtil.gJson2Map(exts)
      targetID = extMap.getOrElse(QRealTimeConstant.KEY_TARGET_ID, "").toString
    }
    //封装返回
    UserLogPageViewData(sid, userDevice, userDeviceType, os,
      userID, userRegion, userRegionIP, lonitude, latitude,
      manufacturer, carrier, networkType, duration,
      action, eventType, ct, targetID)
  }
}
