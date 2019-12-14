package com.qf.bigdata.realtime.flink.util.help

import scala.collection.mutable.ArrayBuffer

object MetaDataHelper {

  /**
    * 广告投放元数据
    * @return
    */
  def getReleaseColumns():Array[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("sid")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("exts")
    columns.+=("ct")
    columns.toArray
  }


  /**
    * 渠道通道映射元数据
    * @return
    */
  def getSourceChannelColumns():Array[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("sources_remark")
    columns.+=("channels")
    columns.+=("channels_remark")
    columns.+=("media_type")
    columns.+=("media_type_remark")
    columns.+=("ctime")
    columns.toArray
  }

  /**
    * 广告投放宽表数据
    * @return
    */
  def getReleaseSourceColumns():Array[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("mediaType")
    columns.+=("exts")
    columns.+=("bdp_day")
    columns.toArray
  }



}
