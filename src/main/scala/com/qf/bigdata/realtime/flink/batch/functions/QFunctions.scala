package com.qf.bigdata.realtime.flink.batch.functions

import com.qf.bigdata.realtime.bean.{ReleaseBean, SourcesBean}
import com.qf.bigdata.realtime.constant.TravelConstant
import com.qf.bigdata.realtime.flink.batch.ReleaseBatchJob.{MinRelease, MinReleaseSource}
import com.qf.bigdata.realtime.util.CommonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.{MapFunction, RichFilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration

/**
  * 自定义函数
  */
object QFunctions {


  /**
    * 自定义map转换函数
    */
  class ToTupleMapFun extends MapFunction[(String,String,String,String,Long,String,Long),(String,String,Long,String)]{
    override def map(value: (String, String, String, String, Long, String, Long)): (String,String,Long,String) = {
      val sources = value._1
      val channels = value._3
      val mediaType = value._5
      val ctime = value._7
      val day = CommonUtil.formatDate4Timestamp(ctime, TravelConstant.DEF_FORMAT)
      (sources, channels, mediaType, day)
    }
  }

  /**
    * 自定义map转换函数(case class)
    */
  class ToCaseClassMapFun extends MapFunction[(String,String,String,String,Long,String,Long),MinRelease]{
    override def map(value: (String, String, String, String, Long, String, Long)): MinRelease = {
      val sources = value._1
      val channels = value._3
      val mediaType = value._5
      val ctime = value._7
      val day = CommonUtil.formatDate4Timestamp(ctime, TravelConstant.DEF_FORMAT)
      MinRelease(sources, channels, mediaType, day)
    }
  }


  /**
    * 自定义filter过滤函数(包含匹配)
    */
  class QFContainFilterFun(ranges :String, split:String) extends RichFilterFunction[(String,String,String,String,Long,String,Long)]{

    val sepValues :Seq[String] = ranges.split(split)

    override def filter(value: (String, String, String, String, Long, String, Long)): Boolean = {
      val sources = value._1
      var result = false
      if(StringUtils.isNotEmpty(sources)){
        result = sepValues.contains(sources)
      }
      result
    }
  }


  /**
    * 自定义map转换函数
    * 外部数据集合通过参数传递进行计算
    */
  class QFReleaseMapFun(sourceBroadcastName :String) extends RichMapFunction[ReleaseBean,MinReleaseSource] {

    import scala.collection.JavaConversions._

    /**
      * 渠道通道数据
      */
    var sourcesSets: Traversable[SourcesBean] = null

    /**
      * 初始化准备
      * @param config
      */
    override def open(config: Configuration): Unit = {
      sourcesSets = getRuntimeContext().getBroadcastVariable[SourcesBean](sourceBroadcastName)
    }

    /**
      * 转换函数
      * @param value
      * @return
      */
    override def map(release: ReleaseBean): MinReleaseSource = {
      val sources = release.getSources
      val channels = release.getChannels
      val deviceNum = release.getDevice_num
      val deviceType = release.getDevice_type
      val releaseSession = release.getRelease_session
      val releaseStatus = release.getRelease_status
      val day = CommonUtil.formatDate4Timestamp(release.getCt, TravelConstant.DEF_FORMAT)

      val matchSource :Option[SourcesBean] = sourcesSets.find(sourceBean => {
        var result = false
        val scSource = sourceBean.getSources
        if(sources.equalsIgnoreCase(scSource)){
          result = true
        }
        result
      })

      //媒体类型赋值
      val mediaTypeTmp = matchSource match {
        case Some(s) => s.getMedia_type
        case None => "0"
      }
      val mediaType = mediaTypeTmp.toInt

      MinReleaseSource(sources, channels, mediaType, deviceNum, deviceType, releaseSession, releaseStatus, day)
    }

  }


}
