package com.qianfeng.bigdata.realtime.flink.streaming.sink.logs

import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qianfeng.bigdata.realtime.flink.util.es.ES6ClientUtil
import com.qianfeng.bigdata.realtime.util.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

//用户行为点击日志---essink
class UserLogsClickESSink(indexName:String) extends RichSinkFunction[UserLogClickData]{

  //日志记录
  val logger :Logger = LoggerFactory.getLogger(this.getClass)

  //ES客户端连接对象
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
  override def invoke(value: UserLogClickData, context: SinkFunction.Context[_]): Unit = {
    try {
      //参数校验
      val result :java.util.Map[String,Object] = JsonUtil.gObject2Map(value)
      val checkResult: String = checkData(result)
      if (StringUtils.isNotBlank(checkResult)) {
        //日志记录
        logger.error("Travel.ESRecord.sink.checkData.err{}", checkResult)
        return
      }


      //请求id
      val sid = value.sid

      //索引名称、类型名称
      handleData(indexName, indexName, sid, result)

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
                 value: java.util.Map[String,Object]): Unit ={
    val indexRequest = new IndexRequest(idxName, idxName, esID)
      .source(value)
    val response = transportClient.prepareUpdate(idxName, idxName, esID)
      .setRetryOnConflict(QRealTimeConstant.ES_RETRY_NUMBER)
      .setDoc(value)
      .setUpsert(indexRequest)
      .get()
    println(response.status().getStatus+"=====================")
    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      //ObjectMapper引入databind包中
      System.err.println("calculate es session record error!map:" + new ObjectMapper().writeValueAsString(value))
      throw new Exception("run exception:status:" + response.status().name())
    }
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
  def checkData(value :java.util.Map[String,Object]): String ={
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
    val eventType = value.get(QRealTimeConstant.KEY_EVENT_TYPE)
    if(null == eventType){
      msg = "Travel.ESSink.eventtype  is null"
    }

    //时间
    val ctNode = value.get(QRealTimeConstant.CT)
    if(null == ctNode){
      msg = "Travel.ESSink.ct is null"
    }

    msg
  }
}
