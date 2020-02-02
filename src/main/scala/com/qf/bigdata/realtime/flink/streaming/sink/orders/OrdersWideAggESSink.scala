package com.qf.bigdata.realtime.flink.streaming.sink.orders

import com.fasterxml.jackson.databind.ObjectMapper
import com.qf.bigdata.realtime.constant.CommonConstant
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.{OrderWideData}
import com.qf.bigdata.realtime.flink.util.es.ES6ClientUtil
import com.qf.bigdata.realtime.util.json.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * 自定义ES Sink
  * 对应业务点：基于订单宽表明细累计输出
  */
class OrdersWideAggESSink(indexName:String) extends RichSinkFunction[OrderWideData]{


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
  override def invoke(value: OrderWideData, context: SinkFunction.Context[_]): Unit = {
    try {
      //参数校验
      val orderDataJson:String = JsonUtil.gObject2Json(value)
      val checkResult: String = checkData(value)
      if (StringUtils.isNotBlank(checkResult)) {
        //日志记录
        logger.error("Travel.order.ESRecord.sink.checkData.err{}", checkResult)
        return
      }

      //行为+事件类型+用户地区+手机制造商+电信运营商
      val sep = CommonConstant.BOTTOM_LINE
      val productType = value.productType
      val depCode = value.depCode
      val triffic = value.traffic
      val id = depCode+sep+productType+sep+triffic

      //加入更新时间
      val record :java.util.Map[String,Object] = JsonUtil.json2object(orderDataJson, classOf[java.util.Map[String,Object]])

      //索引名称、类型名称
      val orderFields :List[String] = getOrderWideAggUseFields()
      handleData(indexName, indexName, id, record, orderFields)

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
  def handleOrderData(idxName :String, idxTypeName :String, esID :String,
                 value: java.util.Map[String,String]): Unit ={
    val indexRequest = new IndexRequest(idxName, idxName, esID).source(value)
    val response = transportClient.prepareUpdate(idxName, idxName, esID)
      .setRetryOnConflict(QRealTimeConstant.ES_RETRY_NUMBER)
      .setDoc(value)
      .setUpsert(indexRequest)
      .get()
    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      System.err.println("calculate es session record error!map:" + new ObjectMapper().writeValueAsString(value))
      throw new Exception("run exception:status:" + response.status().name())
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
    val scriptSb: StringBuilder = new StringBuilder
    //println(s"""value=${value.keys.mkString}""")
    for ((k :String, v:Object) <- value ) { //if(null != k); if(null != v)
      params.put(k, v)
      //println(s"""kkk==>${k}""")

      var s = ""
      if(QRealTimeConstant.KEY_CT.equals(k)) {
        s = "if(ctx._source."+k+" == null){ctx._source."+k+" = params."+k+"} else { if(ctx._source."+k+" < params."+k+" ){ctx._source."+k+" = params."+k+"}}"
      }else if(QRealTimeConstant.POJO_FIELD_FEE.equalsIgnoreCase(k)){
        //累计费用
        s = " if(ctx._source."+k+" != null) {ctx._source."+k +"+= params." + k + "} else { ctx._source."+k+" = params."+k+"} "
      }else if(QRealTimeConstant.POJO_FIELD_FEE_MAX.equalsIgnoreCase(k)){
        //最大值
        s = " if(ctx._source."+k+" == null){ctx._source."+k+" = params."+k+"} else { if(ctx._source."+k+" < params."+k+" ){ctx._source."+k+" = params."+k+"}}"

      }else if(QRealTimeConstant.POJO_FIELD_FEE_MIN.equalsIgnoreCase(k)){
        //最小值
        s = " if(ctx._source."+k+" == null){ctx._source."+k+" = params."+k+"} else { if(ctx._source."+k+" > params."+k+" ){ctx._source."+k+" = params."+k+"}}"

      }else if(QRealTimeConstant.POJO_FIELD_MEMBERS.equalsIgnoreCase(k)){
        //累计出行人数
        s = " if(ctx._source."+k+" != null) {ctx._source."+k +"+= params." + k + "} else { ctx._source."+k+" = params."+k+"} "

      } else if(getOrderWideAggUseFields().contains(k)){
        s = " if(ctx._source."+k+" == null) { ctx._source."+k+" = params."+k+" } "
      }else if(QRealTimeConstant.POJO_FIELD_ORDERS.equalsIgnoreCase(k)){
        //s = " ctx._source."+k +" += params."+ k+" "
        s = " if(ctx._source."+k+" != null) { ctx._source."+k+" = params."+k+" } "
      }
      scriptSb.append(s)
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
    * @return
    */
  def getOrderWideAggUseFields() : List[String] = {
    var useFields :List[String] = List[String]()
    useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_USERREGION)
    useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_TRAFFIC)
    useFields = useFields.:+(QRealTimeConstant.POJO_PRODUCT_DEPCODE)
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
  def checkData(value: OrderWideData): String ={
    var msg = ""
    if(null == value){
      msg = "kafka.value is empty"
    }

    //产品ID
    val productID = value.productID
    if(null == productID){
      msg = "Travel.order.ESSink.productID  is null"
    }

    //出行交通
    val triffic = value.traffic
    if(null == triffic){
      msg = "Travel.order.ESSink.triffic  is null"
    }

    //时间
    val ctNode = value.ct
    if(null == ctNode){
      msg = "Travel.order.ESSink.ct is null"
    }

    msg
  }


}
