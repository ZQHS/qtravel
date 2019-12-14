package com.qf.bigdata.realtime.flink.streaming.etl

import java.util
import java.util.{HashMap, List, Map}

import com.alibaba.fastjson.JSON
import com.google.gson.JsonObject
import com.qf.bigdata.realtime.constant.CommonConstant
import com.qf.bigdata.realtime.enumes.{ActionEnum, EventEnum}
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.functions.QRealTimeDO.{ActionDim, UserLogDWDo, UserLogODSDo}
import com.qf.bigdata.realtime.util.json.{JsonMapperUtil, JsonUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.mutable

/**
  * 原始数据ETL过程涉及函数
  */
object QRealtimeEtlFun {

  /**
    * json解析
    */
  val objectMapper: ObjectMapper = new ObjectMapper()

  class travelDataMapFunOld extends MapFunction[String,String]{

    override def map(value: String): String = {

      val nValue = mutable.Map[String,String]()

      //业务逻辑
      //val record :java.util.Map[String,String] = JsonMapperUtil.readValue(value, classOf[java.util.Map[String,String]])
      val record :java.util.Map[String,String] = JsonUtil.json2object(value, classOf[java.util.Map[String,String]])

      //普通字段
      //请求ID
      val sid :String = record.getOrDefault(QRealTimeConstant.KEY_SID,"");
      nValue.+=(QRealTimeConstant.KEY_SID -> sid)

      //用户ID
      val userID :String = record.getOrDefault(QRealTimeConstant.KEY_USER_ID,"");
      nValue.+=(QRealTimeConstant.KEY_USER_ID -> userID)

      //用户设备号
      val userDevice :String = record.getOrDefault(QRealTimeConstant.KEY_USER_DEVICE,"");
      nValue.+=(QRealTimeConstant.KEY_USER_DEVICE -> userDevice)

      //用户设备类型
      val userDeviceType :String = record.getOrDefault(QRealTimeConstant.KEY_USER_DEVICE_TYPE,"");
      nValue.+=(QRealTimeConstant.KEY_USER_DEVICE_TYPE -> userDeviceType)

      //操作系统
      val os :String = record.getOrDefault(QRealTimeConstant.KEY_OS,"");
      nValue.+=(QRealTimeConstant.KEY_OS -> os)

      //手机制造商
      val manufacturer :String = record.getOrDefault(QRealTimeConstant.KEY_MANUFACTURER,"");
      nValue.+=(QRealTimeConstant.KEY_MANUFACTURER -> manufacturer)

      //电信运营商
      val carrier :String = record.getOrDefault(QRealTimeConstant.KEY_CARRIER,"");
      nValue.+=(QRealTimeConstant.KEY_CARRIER -> carrier)

      //网络类型
      val networkType :String = record.getOrDefault(QRealTimeConstant.KEY_NETWORK_TYPE,"");
      nValue.+=(QRealTimeConstant.KEY_NETWORK_TYPE -> networkType)

      //用户所在地区
      val userRegion :String = record.getOrDefault(QRealTimeConstant.KEY_USER_REGION,"");
      nValue.+=(QRealTimeConstant.KEY_USER_REGION -> userRegion)

      //用户所在地区IP
      val userRegionIP :String = record.getOrDefault(QRealTimeConstant.KEY_USER_REGION_IP,"");
      nValue.+=(QRealTimeConstant.KEY_USER_REGION_IP -> userRegionIP)

      //事件时间
      val ct :String = record.getOrDefault(QRealTimeConstant.KEY_CT,"");
      nValue.+=(QRealTimeConstant.KEY_CT -> ct)

      //根据行为和事件数据进行扩展信息提取
      val action :String = record.getOrDefault(QRealTimeConstant.KEY_ACTION,"");
      val eventType :String = record.getOrDefault(QRealTimeConstant.KEY_EVENT_TYPE,"");
      nValue.+=(QRealTimeConstant.KEY_ACTION -> action)
      nValue.+=(QRealTimeConstant.KEY_EVENT_TYPE -> eventType)

      //APP启动时无扩展信息
      if (ActionEnum.PAGE_ENTER_NATIVE.getCode.equalsIgnoreCase(action)) {
        //无具体扩展信息
      }else {
        val exts :String = record.getOrDefault(QRealTimeConstant.KEY_EXTS,"");
        val extRecord :java.util.Map[String,String] =JsonUtil.json2object(exts, classOf[java.util.Map[String,String]])

        //JsonMapperUtil.readValue(exts, classOf[java.util.Map[String,String]])

        /**
          * 不同行为不同事件提取的扩展信息不一样(具体参加文档说明)
          */
        val targetID :String = extRecord.getOrDefault(QRealTimeConstant.KEY_EXTS_TARGET_ID,"");
        if(EventEnum.VIEW.getCode.equalsIgnoreCase(eventType) || EventEnum.SLIDE.getCode.equalsIgnoreCase(eventType)){//查询或滑动
        val targetIDS :String = extRecord.getOrDefault(QRealTimeConstant.KEY_EXTS_TARGET_IDS,"");
          nValue.+=(QRealTimeConstant.KEY_EXTS_TARGET_IDS -> targetIDS)
        }else{
          nValue.+=(QRealTimeConstant.KEY_EXTS_TARGET_ID -> targetID)
        }
      }

      //val nrecord = JsonMapperUtil.obj2String(nValue)

      import org.apache.flink.api.scala._
      import scala.collection.JavaConversions._
      val nn :java.util.Map[String,String]= nValue
      val nrecord = JsonUtil.object2json(nn)
      println(s"""nrecord=${nrecord}""")
      nrecord
    }
  }


  /**
    * 用户行为原始数据ETL
    * 数据进行规范化处理
    */
  class travelDataMapFun extends MapFunction[String,String]{

    override def map(value: String): String = {
      ///根据行为和事件数据进行扩展信息提取
      //val record :java.util.Map[String,String] = JsonMapperUtil.readValue(value, classOf[java.util.Map[String,String]])
      val record :java.util.Map[String,String] = JsonUtil.json2object(value, classOf[java.util.Map[String,String]])
      val exts :String = record.getOrDefault(QRealTimeConstant.KEY_EXTS,"");
      if(StringUtils.isNotEmpty(exts)){
        val extRecord :java.util.Map[String,String] =JsonUtil.json2object(exts, classOf[java.util.Map[String,String]])
        record.putAll(extRecord)
      }
      val nrecord = JsonUtil.object2json(record)
      //println(s"""map.nrecord=${nrecord}""")
      nrecord
    }

  }

  /**
    * 用户行为原始数据ETL
    * 数据扁平化处理：浏览多产品记录拉平
    */
  class travelDataFlatMapFun extends FlatMapFunction[String,String]{

    override def flatMap(value: String, values: Collector[String]): Unit = {
      //根据行为和事件数据进行扩展信息提取
      val record :java.util.Map[String,String] = JsonUtil.json2object(value, classOf[java.util.Map[String,String]])

      //行为和事件
      val sid :String = record.getOrDefault(QRealTimeConstant.KEY_SID,"");
      val action :String = record.getOrDefault(QRealTimeConstant.KEY_ACTION,"");
      val eventType :String = record.getOrDefault(QRealTimeConstant.KEY_EVENT_TYPE,"");

      //交互式查询中存在浏览多产品信息情况
      if (ActionEnum.INTERACTIVE.getCode.equalsIgnoreCase(action)) {
        if (EventEnum.VIEW.getCode.equalsIgnoreCase(eventType) || EventEnum.SLIDE.getCode.equalsIgnoreCase(eventType)){
            val exts :String = record.getOrDefault(QRealTimeConstant.KEY_EXTS,"");
            val extRecord :java.util.Map[String,String] =JsonUtil.json2object(exts, classOf[java.util.Map[String,String]])

            val targetIDS :Array[String] = extRecord.getOrDefault(QRealTimeConstant.KEY_EXTS_TARGET_IDS,"").split(CommonConstant.COMMA)
            for(targetID <- targetIDS){
               val nRecord : java.util.Map[String,String] = new java.util.HashMap[String,String]()
               nRecord.putAll(record)
               nRecord.put(QRealTimeConstant.KEY_EXTS_TARGET_ID, targetID)
               nRecord.remove(QRealTimeConstant.KEY_EXTS_TARGET_IDS)

               val reJson = JsonUtil.object2json(nRecord)
               println(s"""flatMap.sid="${sid}",reJson=${reJson}""")
               values.collect(reJson)
            }
        }
      }else{
        val reJson = JsonUtil.object2json(record)
        println(s"""flatMap.reJson=${reJson}""")
        values.collect(reJson)
      }
    }
  }


  /**
    * 用户行为原始数据ETL(宽表)
    */
  class travelWideDataMapFun extends RichMapFunction[String,UserLogDWDo]{

    //隐式转换
    import scala.collection.JavaConversions._

    /**
      * 行为数据
      */
    var actions: Traversable[ActionDim] = null


    /**
      * 初始化准备
      * @param config
      */
    override def open(config: Configuration): Unit = {
      actions = getRuntimeContext().getBroadcastVariable[ActionDim](QRealTimeConstant.BC_ACTIONS)
    }


    /**
      * 转换处理->宽表数据
      * @param value
      * @return
      */
    override def map(value: String): UserLogDWDo = {
      val userLogODSDo :UserLogODSDo = objectMapper.readValue(value, classOf[UserLogODSDo])

      //事件新闻
      val action = userLogODSDo.action

      val sid:String = userLogODSDo.sid
      val device:String = userLogODSDo.device
      val deviceType:String = userLogODSDo.deviceType
      val os:String = userLogODSDo.os
      val userID:String = userLogODSDo.userID
      val userRegion:String = userLogODSDo.userRegion
      val lon:String = userLogODSDo.lon
      val lat:String = userLogODSDo.lat
      val geoHashCode:String = lon+","+lat
      val ct :Long = userLogODSDo.ct.toLong
      val eventType:String = userLogODSDo.eventType;

      //扩展信息
      val exts :String = userLogODSDo.exts
      val extsDict :Map[String,Any] = objectMapper.readValue(exts, classOf[Map[String,Any]])
      val targetID = extsDict.getOrElse[Any](QRealTimeConstant.KEY_EXTS_TARGET_ID, "").toString

      //关联信息
      val actionDim : Option[ActionDim]= actions.find(_.code.equalsIgnoreCase(action))
      val actionRemark :String = actionDim match {
        case Some(a) => a.remark
        case None => "Unkown"
      }

//      UserLogDWDo(sid, device, deviceType, os,
//        userID,userRegion, geoHashCode, ct,
//        action, actionRemark, eventType, targetID)

      null
    }
  }




  class ProducerStringSerializationSchema(var topic: String) extends KafkaSerializationSchema[String] {
    override def serialize(element: String, timestamp: java.lang.Long) =
      new ProducerRecord[Array[Byte], Array[Byte]](topic,
        element.getBytes())
  }


}
