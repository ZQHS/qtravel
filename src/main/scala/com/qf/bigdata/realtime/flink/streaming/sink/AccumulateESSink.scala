package com.qf.bigdata.realtime.flink.streaming.sink

import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import com.qf.bigdata.realtime.constant.TravelConstant
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.functions.QRealTimeDO.{UserLogAggDo, UserLogDWDo}
import com.qf.bigdata.realtime.flink.util.es.ES6ClientUtil
import com.qf.bigdata.realtime.util.json.{JsonMapperUtil, JsonUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * 自定义ES Sink
  * 数据累计实时展示
  *
  * @param indexName
  */
class AccumulateESSink (indexName:String) extends RichSinkFunction[java.util.Map[String,Object]]{

  val logger :Logger = LoggerFactory.getLogger(this.getClass)

  var transportClient: PreBuiltTransportClient = _


  /**
    * 连接es集群
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    //flink与es网络通信参数设置(默认虚核)
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    transportClient = ES6ClientUtil.buildTransportClient()
    super.open(parameters)
  }


  /**
    * Sink输出处理
    * @param value
    * @param context
    */
  override def invoke(value: java.util.Map[String,Object], context: SinkFunction.Context[_]): Unit = {
    try {
      //参数校验
      val checkResult: String = checkData(value)
      if (StringUtils.isNotBlank(checkResult)) {
        //日志记录
        logger.error("Travel.ESRecord.sink.checkData.err{}", checkResult)
        return
      }

      //会话ID
      val sid = value.get(QRealTimeConstant.KEY_SID).toString
      if(StringUtils.isNotEmpty(sid)){
        //加入更新时间
        value.put(QRealTimeConstant.KEY_UT, new Date().getTime.toString)

        //拼凑ID
        val action = value.get(QRealTimeConstant.KEY_ACTION)
        val eventType = value.get(QRealTimeConstant.KEY_EVENT_TYPE)
        val userRegion = value.get(QRealTimeConstant.KEY_USER_REGION)
        val id = action + "_" + eventType
        value.put(QRealTimeConstant.KEY_ES_ID, id)

        //累计和
        import scala.collection.JavaConversions._
        val duration = value.getOrDefault(QRealTimeConstant.KEY_DURATION, "").toString
        val dInt:java.lang.Integer = duration.toInt
        value.put(QRealTimeConstant.KEY_DURATION, dInt)

        //索引名称、类型名称
        import scala.collection.JavaConversions._
        val fields :List[String] = getAggUseFields()
        handleData(indexName, indexName, id, value, fields)
      }

    }catch{
      case ex: Exception => logger.error(ex.getMessage)
    }
  }

  /**
    * ES插入或更新数据
    * @param idxName
    * @param idxTypeName
    * @param esID
    * @param value
    */
  def handleData(idxName :String, idxTypeName :String, esID :String,
                    value: mutable.Map[String,Object], fields:List[String]): Unit ={
    //脚本参数赋值
    val params = new java.util.HashMap[String, Object]

    /**
      * 本示例以【action】【eventType】【userRegion】为分组纬度
      * 度量：
      *   1 pv 访问量 -> sid
      *   2 最大停留时长 -> duration (暂时模拟，最后用价格等数据替换)
      */
      import org.apache.flink.api.scala._
      val scriptSb: StringBuilder = new StringBuilder
      println(s"""value=${value.keys.mkString}""")

      for ((k :String, v:String) <- value if(null != k); if(null != v)) {
        params.put(k, v)
        println(s"""kkk==>${k}""")


      var s = ""
      var s2 = ""
      if(QRealTimeConstant.KEY_UT.equals(k)) {
        s = "if(ctx._source."+k+" == null){ctx._source."+k+" = params."+k+"} else { if(ctx._source."+k+" < params."+k+" ){ctx._source."+k+" = params."+k+"}}"
      }else if(QRealTimeConstant.KEY_SID.equalsIgnoreCase(k)){

        s = "if(ctx._source."+QRealTimeConstant.ES_PV+" != null) {ctx._source."+QRealTimeConstant.ES_PV +" += 1 } else { ctx._source."+QRealTimeConstant.ES_PV+" = 1 } "
        params.remove(QRealTimeConstant.KEY_SID)

      }else if(QRealTimeConstant.KEY_DURATION.equalsIgnoreCase(k)){
        //pv访问量
        s = "if(ctx._source."+k+" != null) {ctx._source."+k +"+= params." + k + "} else { ctx._source."+k+" = params."+k+"} "
        //最大值
        val subk = QRealTimeConstant.ES_MAX
        s2 = "if(ctx._source."+subk+" == null){ctx._source."+subk+" = params."+k+"} else { if(ctx._source."+subk+" < params."+k+" ){ctx._source."+subk+" = params."+k+"}}"
      } else if(getAggGroupFields.contains(k)){
        s = " if (ctx._source."+k+" == null) { ctx._source."+k+" = params."+k+" } "
      }
      scriptSb.append(s)
      scriptSb.append(s2)
    }

    val scripts = scriptSb.toString()
    val script = new Script(ScriptType.INLINE, "painless", scripts, params)
    println(s"script=$script")

    val indexRequest = new IndexRequest(idxName, idxTypeName, esID).source(params)
    val response = transportClient.prepareUpdate(idxName, idxTypeName, esID)
      .setScript(script)
      .setRetryOnConflict(QRealTimeConstant.ES_RETRY_NUMBER)
      .setUpsert(indexRequest)
      .get()
    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      System.err.println("calculate es session record error!map:" + new ObjectMapper().writeValueAsString(value))
      throw new Exception("run script exception:status:" + response.status().name())
    }
  }


  /**
    * 选择参与计算的维度和度量
    * 模拟使用【action】【eventType】【userRegion】【os】为分组纬度
    * 度量使用【sid】【duration】
    * @return
    */
  def getAggUseFields() : List[String] = {
    var useFields :List[String] = List[String]()
    useFields = useFields.:+(QRealTimeConstant.KEY_ES_ID)
    useFields = useFields.:+(QRealTimeConstant.KEY_SID)
    useFields = useFields.:+(QRealTimeConstant.KEY_ACTION)
    useFields = useFields.:+(QRealTimeConstant.KEY_EVENT_TYPE)
    useFields = useFields.:+(QRealTimeConstant.KEY_USER_REGION)
    useFields = useFields.:+(QRealTimeConstant.KEY_DURATION)
    useFields
  }

  def getAggGroupFields() : List[String] = {
    var useFields :List[String] = List[String]()
    useFields = useFields.:+(QRealTimeConstant.KEY_ES_ID)
    useFields = useFields.:+(QRealTimeConstant.KEY_ACTION)
    useFields = useFields.:+(QRealTimeConstant.KEY_EVENT_TYPE)
    useFields
  }


  /**
    * 资源关闭
    */
  override def close() = {
    if (this.transportClient != null) {
      this.transportClient.close()
    }
  }


  /**
    * 参数校验
    * @param value
    * @return
    */
  def checkData(value: java.util.Map[String,Object]): String ={
    var msg = ""
    if(null == value){
      msg = "kafka.value is empty"
    }

    //行为类型
    val action = value.get(QRealTimeConstant.KEY_ACTION)
    if(null == action){
      msg = "Travel.ESSink.action  is null"
    }

    //行为类型
    val eventTyoe = value.get(QRealTimeConstant.KEY_EVENT_TYPE)
    if(null == eventTyoe){
      msg = "Travel.ESSink.eventtype  is null"
    }

    //地区
    val userRegion = value.get(QRealTimeConstant.KEY_USER_REGION)
    if(null == userRegion){
      msg = "Travel.ESSink.userRegion  is null"
    }

    //会话ID
    val sid = value.get(QRealTimeConstant.KEY_SID)
    if(null == sid){
      msg = "Travel.ESSink.sid  is null"
    }

    //时间
    val ctNode = value.get(QRealTimeConstant.KEY_CT)
    if(null == ctNode){
      msg = "Travel.ESSink.ct is null"
    }

    msg
  }


}
