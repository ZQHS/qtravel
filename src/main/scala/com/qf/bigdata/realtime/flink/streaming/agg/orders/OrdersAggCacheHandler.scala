package com.qf.bigdata.realtime.flink.streaming.agg.orders

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.agg.mapper.QRedisSetMapper
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersAggFun.{OrderDetailTimeAggFun, OrderDetailTimeWindowFun, OrderStatisWindowProcessFun}
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.json.JsonUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink

/**
  * 订单数据
  * 实时聚合结果写入缓存供外部系统使用
  */
object OrdersAggCacheHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("OrdersAggCacheHandler")

  /**
    * 旅游产品订单数据实时ETL
    * @param appName 程序名称
    * @param fromTopic 数据源输入 kafka topic
    * @param groupID 消费组id
    * @param indexName 数据流输出
    */
  def handleOrders4RedisJob(appName:String, groupID:String, fromTopic:String, redisDB:Int):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      //注意：检查点时间间隔单位：毫秒
      val checkpointInterval = QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL
      val watermarkInterval= QRealTimeConstant.FLINK_WATERMARK_INTERVAL
      val timeChar = TimeCharacteristic.EventTime
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(checkpointInterval, timeChar, watermarkInterval)

      /**
        * 2 读取kafka旅游产品订单数据并形成订单实时数据流
        */
      val orderDetailDStream:DataStream[OrderDetailData] = FlinkHelper.createOrderDetailDStream(env, groupID, fromTopic,timeChar)

      /**
        * 3 开窗聚合操作
        * (1) 分组维度列：用户所在地区(userRegion),出游交通方式(traffic)
        * (2) 聚合结果数据(分组维度+度量值)：OrderDetailTimeAggDimMeaData
        * (3) 开窗方式：滚动窗口TumblingEventTimeWindows
        * (4) 允许数据延迟：allowedLateness
        * (5) 聚合计算方式：aggregate
        */
      val aggDStream:DataStream[QKVBase] = orderDetailDStream
        .keyBy(
          (detail:OrderDetailData) => OrderDetailAggDimData(detail.userRegion, detail.traffic)
        )
        .window(TumblingEventTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE)))
        .allowedLateness(Time.seconds(QRealTimeConstant.FLINK_ALLOWED_LATENESS))
        .aggregate(new OrderDetailTimeAggFun(), new OrderDetailTimeWindowFun())
          .map(
            (data:OrderDetailTimeAggDimMeaData) => {
              val key = data.userRegion + "_" + data.traffic
              val value = JsonUtil.gObject2Json(data)
              QKVBase(key, value)
            }
          )
      aggDStream.print("order.aggDStream  ---:")


      /**
        * 4 写入下游环节缓存redis(被其他系统调用)
        */
      val redisMapper = new QRedisSetMapper()
      val redisConf = FlinkHelper.createRedisConfig(redisDB)
      val redisSink = new RedisSink(redisConf, redisMapper)
      aggDStream.addSink(redisSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersAggCacheHandler.err:" + ex.getMessage)
      }
    }

  }


  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)

    //应用程序名称
    val appName = "qf.OrdersAggCacheHandler"
    //kafka消费组
    val groupID = "qf.OrdersAggCacheHandler"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val fromTopic = "test_ods"

    //redis数据库
    val redisDB = 9

    //1 聚合数据输出redis
    handleOrders4RedisJob(appName, groupID, fromTopic, redisDB)

  }


}
