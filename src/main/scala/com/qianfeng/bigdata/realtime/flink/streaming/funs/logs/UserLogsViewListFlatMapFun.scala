package com.qianfeng.bigdata.realtime.flink.streaming.funs.logs

import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qianfeng.bigdata.realtime.util.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.JavaConversions._
import java.util

/**
 * 用户行为原始数据ETL
 * 数据扁平化处理：浏览多产品记录拉平
 */
class UserLogsViewListFlatMapFun  extends FlatMapFunction[UserLogViewListData,UserLogViewListFactData]{

  override def flatMap(value: UserLogViewListData, values: Collector[UserLogViewListFactData]): Unit = {

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
    val duration :String = value.duration
    val action:String = value.action
    val eventType:String = value.eventType
    val ct:Long = value.ct
    val hotTarget:String = value.hotTarget
    val exts :String = value.exts

    if(StringUtils.isNotEmpty(exts)){
      val extMap :mutable.Map[String,AnyRef] = JsonUtil.gJson2Map(exts)
      val travelSendTime = extMap.getOrElse(QRealTimeConstant.KEY_TRAVEL_SENDTIME, "").toString
      val travelSend = extMap.getOrElse(QRealTimeConstant.KEY_TRAVEL_SEND, "").toString
      val travelTime = extMap.getOrElse(QRealTimeConstant.KEY_TRAVEL_TIME, "").toString
      val productLevel = extMap.getOrElse(QRealTimeConstant.KEY_PRODUCT_LEVEL, "").toString
      val productType = extMap.getOrElse(QRealTimeConstant.KEY_PRODUCT_TYPE, "").toString
      val targetIDSJson = extMap.getOrElse(QRealTimeConstant.KEY_TARGET_IDS, "").toString

      //列表拆分
      val targetIDS: util.List[String] = JsonUtil.gJson2List(targetIDSJson)
      //注意，将targetIDS转换成scala的list咯
      for(targetID <- targetIDS){
        val data = UserLogViewListFactData(sid, userDevice, userDeviceType, os,
          userID,userRegion,userRegionIP, lonitude, latitude,
          manufacturer, carrier, networkType, duration,
          action, eventType, ct,
          targetID, hotTarget, travelSend, travelSendTime,
          travelTime, productLevel, productType)

        //数仓
        values.collect(data)
      }
    }
  }
}
