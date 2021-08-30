package com.qianfeng.bigdata.realtime.flink.streaming.funs.logs

import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qianfeng.bigdata.realtime.util.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.MapFunction

import scala.collection.mutable
import scala.collection.JavaConversions._


//用户行为日志转换为用户行为点击数据
class UserLogClickDataMapFun extends MapFunction[UserLogData,UserLogClickData]{

  override def map(value: UserLogData): UserLogClickData = {
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
    val action:String = value.action
    val eventType:String = value.eventType
    val ct:Long = value.ct
    val exts :String = value.exts
    var targetID :String = ""
    var eventTargetType :String = ""
    if(StringUtils.isNotEmpty(exts)){
      //需要引入java和scala相互转换依赖
      val extMap :mutable.Map[String,AnyRef] = JsonUtil.gJson2Map(exts) // 解析JSON串，转换成Map集合

      targetID = extMap.getOrElse(QRealTimeConstant.KEY_TARGET_ID, "").toString
      eventTargetType = extMap.getOrElse(QRealTimeConstant.KEY_EVENT_TARGET_TYPE, "").toString
    }
    //封装交互行为点击事件数据
    UserLogClickData(sid, userDevice, userDeviceType, os,
      userID,userRegion, userRegionIP, lonitude, latitude,
      manufacturer, carrier, networkType,
      action, eventType, ct, targetID, eventTargetType)
  }
}

