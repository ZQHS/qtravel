package com.qf.bigdata.realtime.flink.streaming.sink.orders

import com.fasterxml.jackson.databind.ObjectMapper
import com.qf.bigdata.realtime.constant.TravelConstant
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{OrderTrafficDimMeaData, OrderWideData, UserLogPageViewData}
import com.qf.bigdata.realtime.flink.util.es.ES6ClientUtil
import com.qf.bigdata.realtime.util.CommonUtil
import com.qf.bigdata.realtime.util.json.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

/**
  * 自定义ES Sink(数据元素以case class)
  * 基于订单统计数据输出
  */
class OrderStatisESSink(indexName:String) extends RichSinkFunction[OrderTrafficDimMeaData] {


  //日志记录
  val logger :Logger = LoggerFactory.getLogger(this.getClass)

  //ES客户端连接对象
  var transportClient: PreBuiltTransportClient = _


  /**
    * 初始化：连接ES集群
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
  override def invoke(value: OrderTrafficDimMeaData, context: SinkFunction.Context[_]): Unit = {
    try {
      //参数校验
      val viewData = JsonUtil.object2json(value)
      val checkResult: String = checkData(viewData)
      if (StringUtils.isNotBlank(checkResult)) {
        logger.error("Travel.ESRecord.sink.checkData.err{}", checkResult)
        return
      }

      //转换数据格式为ES支持的map结构
      val record :java.util.Map[String,String] = JsonUtil.json2object(viewData, classOf[java.util.Map[String,String]])

      //创建ES索引ID
      val productID = value.productID
      val traffic = value.traffic
      val tmp = productID + traffic
      val id = CommonUtil.getMD5AsHex(tmp.getBytes)

      //将数据写入ES集群
      handleData(indexName, indexName, id, record)

    }catch{
      case ex: Exception => logger.error(ex.getMessage)
    }
  }

  /**
    * ES插入或更新数据
    * @param idxName 索引名称
    * @param idxTypeName 索引类型名
    * @param esID 索引ID
    * @param value 写入数据
    */
  def handleData(idxName :String, idxTypeName :String, esID :String,
                 value: java.util.Map[String,String]): Unit ={
    //es索引操作对象
    val indexRequest = new IndexRequest(idxName, idxName, esID).source(value)
    //es更新操作结果
    val response = transportClient.prepareUpdate(idxName, idxName, esID)
      .setRetryOnConflict(QRealTimeConstant.ES_RETRY_NUMBER)
      .setDoc(value)
      .setUpsert(indexRequest)
      .get()
    //写入异常处理
    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      logger.error("calculate es session record error!map:" + new ObjectMapper().writeValueAsString(value))
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
  def checkData(value: String): String ={
    var msg = ""
    if(null == value){
      msg = "kafka.value is empty"
    }

    //转换为Map结构
    val record :java.util.Map[String,String] = JsonUtil.json2object(value, classOf[java.util.Map[String,String]])

    //行为类型
    val action = record.get(QRealTimeConstant.KEY_ACTION)
    if(null == action){
      msg = "Travel.ESSink.action  is null"
    }

    //行为类型
    val eventTyoe = record.get(QRealTimeConstant.KEY_EVENT_TYPE)
    if(null == eventTyoe){
      msg = "Travel.ESSink.eventtype  is null"
    }

    //时间
    val ctNode = record.get(TravelConstant.CT)
    if(null == ctNode){
      msg = "Travel.ESSink.ct is null"
    }

    msg
  }

}
