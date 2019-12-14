package com.qf.bigdata.realtime.flink.streaming.sink

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.qf.bigdata.realtime.constant.TravelConstant
import com.qf.bigdata.realtime.enumes.BusyDBEnum
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.util.kafka.KafkaConfigUtil
import com.qf.bigdata.realtime.flink.util.kafka.KafkaConfigUtil.KafkaConfig
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * 投放实时处理
  * 按投放环节分别统计写入ES进行实时展示
  */
object EsReleaseCalculate {



  val objectNodeValueKey = "value"
  val objectNodeKey = "key"

  val biddingPriceKey = "biddingPrice"
  val winPriceKey = "winPrice"

  val parallelism = 1

  val objectMapper: ObjectMapper = new ObjectMapper()

  val logger :Logger = LoggerFactory.getLogger("EsReleaseCalculate")


  def main(args: Array[String]): Unit = {

      val kafkaConfig: KafkaConfig = KafkaConfigUtil.getConfig(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL) //kafka配置

      //1 flink环境初始化
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.enableCheckpointing(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)

      //2 source数据输入源定义
      val schema = new SimpleStringSchema()
      // schema= new JSONKeyValueDeserializationSchema()


      val consumer = new FlinkKafkaConsumer[String](kafkaConfig.kts, schema, kafkaConfig.kp)
      consumer.setStartFromLatest()


      //3 数据集合操作
      val sessionStream: DataStream[mutable.Map[String, Object]] = env.addSource(consumer).filter { msg =>
        filter(msg)
      }.map[mutable.Map[String, Object]]{ msg =>
        buildBiddingPrice(msg)
      }

      //4 sink数据输出源定义
      //sessionStream.addSink(new EsSink()).setParallelism(parallelism).name("EsReleaseSink")

      //5 启动执行
      env.execute("com.online.demo.EsReleaseCalculate")




  }

  /**
    * 只处理投放业务的竞价环节
    * @param msg
    * @return
    */
  def filter(msg: String): Boolean = {
    var filter: Boolean = false
    try{
      if (StringUtils.isNotBlank(msg)) {

        //多业务同topic过滤
//        val kvs : JSONObject = JSON.parseObject(msg)
//        val dbTable :String = kvs.getString(TravelConstant.DB_TABLE)
//        if(TravelConstant.DBTABLE_BIDDING.equalsIgnoreCase(dbTable)){
//          filter = true
//        }

        filter = true
      }
    }catch{
      case ex:Exception => logger.error("EsReleaseCalculate.filter.err:" + ex.getMessage)
    }

    filter
  }




  /**
    * 构建竞价信息
    * @param msg
    * @return
    */
  def buildBiddingPrice(msg: String) : mutable.Map[String, Object] = {
    var biddingPriceDatas = mutable.Map[String, Object]()
    var errors = ""
    try{
      val msgJson: JsonNode = objectMapper.readTree(msg)

      //方法1
      //        val msgJson: JsonNode = objectMapper.readTree(msg)
      //        val fieldNames : scala.collection.Iterator[String] = msgJson.fieldNames().asScala
      //        for(fieldName <- fieldNames){
      //           val fieldValue = msgJson.get(fieldName)
      //           println(s"""fieldName=${fieldName},fieldValue=${fieldValue}""")
      //        }

      //方法2
//      val kvs : JSONObject = JSON.parseObject(msg)
//      val kvEntrys = kvs.entrySet().asScala
//      for(kv <- kvEntrys){
//        val key = kv.getKey
//        val value = kv.getValue.toString
//        println(s"""key=${key},value=${value}""")
//      }

      //参数校验
      val checkResult :String = checkData(msgJson)
      if(StringUtils.isNotBlank(checkResult)){
        //日志记录
        errors = "EsReleaseCalculate.buildBiddingPrice.checkerr:" + checkResult
        logger.error(errors)
        throw new Exception(errors)
      }


      //处理竞价
      dealBiddingPrice(msgJson, biddingPriceDatas)


    }catch{
      case ex: Exception => logger.error("release.buildBiddingPrice.err:" + ex)
    }

    biddingPriceDatas
  }



  /**
    * 竞价信息处理
    * @return
    */
  def dealBiddingPrice(msgJson: JsonNode, datas: mutable.Map[String, Object]) :Unit = {

    if (null != msgJson && null != datas) {
      /**
        * 基本信息
        */
      setHashMapValue(msgJson, datas, TravelConstant.SESSION_ID, false)
      setHashMapValue(msgJson, datas, TravelConstant.CT, true)
      setHashMapValue(msgJson, datas, TravelConstant.UT, true)
      setHashMapValue(msgJson, datas, TravelConstant.DEVICENUM, false)
      setHashMapValue(msgJson, datas, TravelConstant.DEVICENUM_TYPE, false)
      setHashMapValue(msgJson, datas, TravelConstant.SOURCES, false)
      setHashMapValue(msgJson, datas, TravelConstant.STATUS, false)



      /**
        * 根据业务扩展
        */
      val biddingEnum: BusyDBEnum = BusyDBEnum.REALEASE_BIDDING
      val dbTable: String = biddingEnum.getDb + TravelConstant.BOTTOM_LINE + biddingEnum.getTable
      setHashMapValue(msgJson, datas, TravelConstant.SOURCES, false)
      datas.put(TravelConstant.DB_TABLE, dbTable)
      datas.put(TravelConstant.BIDDING_COUNT, "0")

    }
  }


  /**
    * 获取消息体里数据
    * @param key
    */
  def getInfos(kvs: Map[String,String], key:String): String ={
    var value = ""
    if (StringUtils.isNotBlank(key)) {
       value = kvs.getOrElse(key,"")
    }
    value
  }


  /**
    * 参数数据
    * @param map
    * @param key
    * @param isNumber
    * @param alias
    * @return
    */
  def setHashMapValue(jsonNode: JsonNode, map: mutable.Map[String, Object], key: String, isNumber: Boolean, alias: String = null) = {
    if (null != jsonNode) {
      val childJsonNode = jsonNode.get(key)
      if (null != childJsonNode && !"".equals(childJsonNode) && !childJsonNode.isNull) {
        var mapKey = key
        if (null != alias) {
          mapKey = alias
        }
        if (isNumber) {
          map.put(mapKey, childJsonNode.numberValue())
        } else {
          map.put(mapKey, childJsonNode.textValue())
        }
      }
    }
  }













  /**
    * 参数校验
    * @param msgJson
    * @return
    */
  def checkData(msgJson: JsonNode): String ={
    var msg = ""
    if(null == msgJson){
       msg = "kafka.value is empty"
    }

    //sessionId
    val sessionId = msgJson.get(TravelConstant.SESSION_ID)
    if(null == sessionId){
      msg = "EsReleaseCalculate.sessionId is null"
    }

    //状态
    val status = msgJson.get(TravelConstant.STATUS)
    if(null == status){
      msg = "EsReleaseCalculate.status is null"
    }

    //时间
    val ct = msgJson.get(TravelConstant.CT)
    if(null == ct){
      msg = "EsReleaseCalculate.ct is null"
    }

    msg
  }

  def convertDate2Int(dayTime :String): Integer ={
     var dateInt = 0
     if(StringUtils.isNotBlank(dayTime)){
        dateInt = dayTime.toInt
     }
     dateInt
  }



}
