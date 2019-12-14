package com.qf.bigdata.realtime.flink.streaming.detail

import java.util.Date

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.functions.QRealTimeDO.{UserLogAggDo, UserLogDWDo}
import com.qf.bigdata.realtime.util.CommonUtil
import com.qf.bigdata.realtime.util.json.JsonUtil
import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.java.tuple.Tuple


/**
  * 明细数据处理
  */
object QRealtimeDetailFun {


  /**
    * 用户行为明细数据
    * 数据进行规范化处理
    */
  class travelDetailPojoMapFun extends MapFunction[String,UserLogDWDo]{

    override def map(value: String): UserLogDWDo = {

      //根据行为和事件数据进行扩展信息提取
      val record :java.util.Map[String,String] = JsonUtil.json2object(value, classOf[java.util.Map[String,String]])

      //请求ID
      val sid :String = record.getOrDefault(QRealTimeConstant.KEY_SID,"")

      //用户ID
      val userID :String = record.getOrDefault(QRealTimeConstant.KEY_USER_ID,"")

      //用户设备号
      val userDevice :String = record.getOrDefault(QRealTimeConstant.KEY_USER_DEVICE,"")

      //用户设备类型
      val userDeviceType :String = record.getOrDefault(QRealTimeConstant.KEY_USER_DEVICE_TYPE,"")

      //操作系统
      val os :String = record.getOrDefault(QRealTimeConstant.KEY_OS,"")

      //手机制造商
      val manufacturer :String = record.getOrDefault(QRealTimeConstant.KEY_MANUFACTURER,"")

      //电信运营商
      val carrier :String = record.getOrDefault(QRealTimeConstant.KEY_CARRIER,"")

      //网络类型
      val networkType :String = record.getOrDefault(QRealTimeConstant.KEY_NETWORK_TYPE,"")

      //用户所在地区
      val userRegion :String = record.getOrDefault(QRealTimeConstant.KEY_USER_REGION,"")

      //用户所在地区IP
      val userRegionIP :String = record.getOrDefault(QRealTimeConstant.KEY_USER_REGION_IP,"")

      //经度
      val lon :String = record.getOrDefault(QRealTimeConstant.KEY_LONGITUDE,"")

      //纬度
      val lat :String = record.getOrDefault(QRealTimeConstant.KEY_LATITUDE,"")

      //停留时长
      val duration :String = record.getOrDefault(QRealTimeConstant.KEY_DURATION,"")

      //行为类型
      val action :String = record.getOrDefault(QRealTimeConstant.KEY_ACTION,"")

      //事件类型
      val eventType :String = record.getOrDefault(QRealTimeConstant.KEY_EVENT_TYPE,"")

      //目标ID
      val targetID :String = record.getOrDefault(QRealTimeConstant.KEY_EXTS_TARGET_ID,"")
      //行程
      val travelTime :String = record.getOrDefault(QRealTimeConstant.KEY_EXTS_QUERY_TRAVEL_TIME,"")
      //旅游产品类型://产品类型：跟团、私家、半自助
      val productType :String = record.getOrDefault(QRealTimeConstant.KEY_EXTS_QUERY_PRODUCT_TYPE,"");
      //目的地
      val hotTarget :String = record.getOrDefault(QRealTimeConstant.KEY_EXTS_QUERY_HOT_TARGET,"")
      //出发地
      val travelSend :String = record.getOrDefault(QRealTimeConstant.KEY_EXTS_QUERY_SEND,"")
      //出发时间
      val travelSendTime :String = record.getOrDefault(QRealTimeConstant.KEY_EXTS_QUERY_SEND_TIME,"")
      //产品级别：钻级
      val productLevel :String = record.getOrDefault(QRealTimeConstant.KEY_EXTS_QUERY_PRODUCT_LEVEL,"")

      //事件时间
      val ct :String = record.getOrDefault(QRealTimeConstant.KEY_CT,"")


      new UserLogDWDo(sid, userDevice, userDeviceType, os,
        userID, userRegion, userRegionIP, lon, lat,
        manufacturer, carrier, networkType, duration,
        action, eventType, ct, targetID,
        travelTime, productType, hotTarget, travelSend,
        travelSendTime, productLevel)
    }

  }


  /**
    * 用户行为明细数据
    * 数据进行规范化处理
    */
  class travelDetailSimpleMapFun extends MapFunction[String,UserLogAggDo]{

    override def map(value: String): UserLogAggDo = {

      //根据行为和事件数据进行扩展信息提取
      val record :java.util.Map[String,String] = JsonUtil.json2object(value, classOf[java.util.Map[String,String]])

      //请求ID
      val sid :String = record.getOrDefault(QRealTimeConstant.KEY_SID,"")

      //用户所在地区
      val userRegion :String = record.getOrDefault(QRealTimeConstant.KEY_USER_REGION,"")

      //停留时长
      val duration :String = record.getOrDefault(QRealTimeConstant.KEY_DURATION,"")

      //行为类型
      val action :String = record.getOrDefault(QRealTimeConstant.KEY_ACTION,"")

      //事件类型
      val eventType :String = record.getOrDefault(QRealTimeConstant.KEY_EVENT_TYPE,"")

      //事件时间
      val ct :String = record.getOrDefault(QRealTimeConstant.KEY_CT,"")

      val keyField = action + "_" + eventType + "_" + userRegion

      UserLogAggDo(keyField, sid, duration, ct)
    }
  }


  /**
    * 用户行为明细数据
    * 数据进行规范化处理
    */
  class travelDetailMapFun extends MapFunction[String,java.util.Map[String,Object]]{

    override def map(value: String): java.util.Map[String,Object] = {
      //根据行为和事件数据进行扩展信息提取
      val record :java.util.Map[String,Object] = JsonUtil.json2object(value, classOf[java.util.Map[String,Object]])
      record
    }

  }





}
