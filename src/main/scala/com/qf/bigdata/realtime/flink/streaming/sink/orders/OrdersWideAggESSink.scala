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
  * 自定义ES Sink(数据元素以json字符串形式可以通用)
  * 订单宽表明细数据供使用方查询
  */
class OrdersWideAggESSink(indexName:String) extends RichSinkFunction[OrderWideData]{

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
    * @param value 数据
    * @param context 函数上下文环境对象
    */
  override def invoke(value: OrderWideData, context: SinkFunction.Context[_]): Unit = {
    try {
      //参数校验
      val orderDataJson:String = JsonUtil.gObject2Json(value)
      val checkResult: String = checkData(value)
      if (StringUtils.isNotBlank(checkResult)) {
        logger.error("Travel.order.ESRecord.sink.checkData.err{}", checkResult)
        return
      }

      //行为+事件类型+用户地区+手机制造商+电信运营商
      val sep = CommonConstant.BOTTOM_LINE
      val productType = value.productType
      val depCode = value.depCode
      val triffic = value.traffic
      //val id = depCode+sep+productType+sep+triffic
      val id = depCode

      //确定处理维度及度量
      val useFields :List[String] = getOrderWideAggUseFields()
      val sources :java.util.Map[String,Object] = JsonUtil.json2object(orderDataJson, classOf[java.util.Map[String,Object]])
      val record :java.util.Map[String,Object] = checkUseFields(sources, useFields)


      //将数据写入ES集群(使用ES局部更新功能累计度量数据)
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
                 value: mutable.Map[String,Object]): Unit ={
    //脚本参数赋值
    val params = new java.util.HashMap[String, Object]
    val scriptSb: StringBuilder = new StringBuilder
    for ((k :String, v:Object) <- value ) { //if(null != k); if(null != v)
      params.put(k, v)
      var s = ""
      if(QRealTimeConstant.KEY_CT.equals(k)) {
        s = "if(ctx._source."+k+" == null){ctx._source."+k+" = params."+k+"} else { if(ctx._source."+k+" < params."+k+" ){ctx._source."+k+" = params."+k+"}}"
      }else if(QRealTimeConstant.POJO_FIELD_FEE.equalsIgnoreCase(k)){
        //订单累计费用
        s = " if(ctx._source."+k+" != null) {ctx._source."+k +"+= params." + k + "} else { ctx._source."+k+" = params."+k+"} "
      }else if(QRealTimeConstant.POJO_FIELD_FEE_MAX.equalsIgnoreCase(k)){
        //单笔订单费用最大值
        s = " if(ctx._source."+k+" == null){ctx._source."+k+" = params."+k+"} else { if(ctx._source."+k+" < params."+k+" ){ctx._source."+k+" = params."+k+"}}"
      }else if(QRealTimeConstant.POJO_FIELD_FEE_MIN.equalsIgnoreCase(k)){
        //单笔订单费用最小值
        s = " if(ctx._source."+k+" == null){ctx._source."+k+" = params."+k+"} else { if(ctx._source."+k+" > params."+k+" ){ctx._source."+k+" = params."+k+"}}"
      }else if(QRealTimeConstant.POJO_FIELD_MEMBERS.equalsIgnoreCase(k)){
        //订单累计出行人数
        s = " if(ctx._source."+k+" != null) {ctx._source."+k +"+= params." + k + "} else { ctx._source."+k+" = params."+k+"} "
      } else if(QRealTimeConstant.POJO_FIELD_ORDERS.equalsIgnoreCase(k)){
        s = " if(ctx._source."+k+" != null) { ctx._source."+k+" = params."+k+" } "
      }
      scriptSb.append(s)
    }

    //执行脚本
    val scripts = scriptSb.toString()
    val script = new Script(ScriptType.INLINE, "painless", scripts, params)
    //println(s"script=$script")

    //ES执行插入或更新操作
    val indexRequest = new IndexRequest(idxName, idxTypeName, esID).source(params)
    val response = transportClient.prepareUpdate(idxName, idxTypeName, esID)
      .setScript(script)
      .setRetryOnConflict(QRealTimeConstant.ES_RETRY_NUMBER)
      .setUpsert(indexRequest)
      .get()
    //写入异常处理
    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      logger.error("calculate es session record error!scripts:" + scripts)
      throw new Exception("run exception:status:" + response.status().name())
    }
  }

  /**
    * 选择参与计算的维度和度量
    * @return
    */
  def getOrderWideAggUseFields() : List[String] = {
    var useFields :List[String] = List[String]()
    //useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_USERREGION)
    //useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_TRAFFIC)
    useFields = useFields.:+(QRealTimeConstant.POJO_PRODUCT_DEPCODE)

    useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_ORDERS)
    useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_MEMBERS)
    useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_FEE_MIN)
    useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_FEE_MAX)
    useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_FEE)
    useFields = useFields.:+(QRealTimeConstant.KEY_CT)
    useFields
  }


  /**
    * 选择参与计算的维度和度量
    * @return
    */
  def checkUseFields(datas:java.util.Map[String,Object], useFields:java.util.List[String]) : java.util.Map[String,Object] = {
    val result :java.util.Map[String,Object] = new java.util.HashMap[String,Object]
    for(field <- useFields){
      if(datas.containsKey(field)){
        val value = datas.getOrDefault(field,null)
        result.put(field, value)
      }
    }
    result
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
