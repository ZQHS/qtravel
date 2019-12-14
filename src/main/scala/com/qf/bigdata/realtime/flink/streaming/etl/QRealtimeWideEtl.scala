package com.qf.bigdata.realtime.flink.streaming.etl

import java.util
import java.util.Properties

import com.qf.bigdata.realtime.constant.TravelConstant
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.etl.QRealtimeEtlFun.{travelDataFlatMapFun, travelDataMapFun}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper.createJDBCInputFormat
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RuntimeContext}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.types.Row
import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

object QRealtimeWideEtl {



  val logger :Logger = LoggerFactory.getLogger("QRealtimeWideEtl")

  /**
    * json解析
    */
  val objectMapper: ObjectMapper = new ObjectMapper()


  /**
    * 业务处理：数据扁平化处理
    * @param data
    */
  def travelDataMap(dStream :DataStream[String]) :DataStream[String] = {
    val mDataStream :DataStream[String]  = dStream.map(new travelDataMapFun())
    mDataStream
  }


  /**
    * 业务处理：数据转换
    * @param data
    */
  def travelDataFlatMap(dStream :DataStream[String]) :DataStream[String] = {
    val mDataStream :DataStream[String]  = dStream.flatMap(new travelDataFlatMapFun())
    mDataStream
  }


  /**
    * 创建jdbc数据源
    * @param properties
    * @param query
    * @param fieldTypes
    * @return
    */
  def createJDBCInputFormatDataStream(env: StreamExecutionEnvironment,properties:Properties, query:String, fieldTypes: Seq[TypeInformation[_]]): DataStream[Row] = {
    //数据格式
    val jdbcInputFormat : JDBCInputFormat = createJDBCInputFormat(properties, query, fieldTypes)
    env.createInput(jdbcInputFormat)
  }


  /**
    * 读取行为维表
    * @param env
    * @return
    */
  def readActionDimDataStream(env: StreamExecutionEnvironment):DataStream[Row] = {
    val jdbcProperties = PropertyUtil.readProperties(TravelConstant.JDBC_CONFIG_PATH)
    val dimQuery :String = "select dim_type, dim_code, dim_remark from nshop.comm_dim where dim_type = 'action'"
    val fieldTypes: Seq[TypeInformation[_]] = Seq[TypeInformation[_]](BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    val dimDStream :DataStream[Row]  = createJDBCInputFormatDataStream(env ,jdbcProperties, dimQuery, fieldTypes)
    dimDStream
  }



  /**
    * 业务处理(选择维度及ETL)
    * @param data
    */
//    def travelWideDataMap(factStream :DataStream[String], dimStream:DataStream[Row]) : Unit = {
//    //广播维表
//    val dimMapStateDesc = new MapStateDescriptor(QRealTimeConstant.BC_ACTIONS, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
//    val dimBCStream :BroadcastStream[Row] = dimStream.broadcast(dimMapStateDesc)
//
//    //宽表数据集:广播式关联
//    val midDataStream :BroadcastConnectedStream[String,Row] = factStream.connect(dimBCStream)
//    val wideDataStream :DataStream[String] = midDataStream.process(
//      new BroadcastProcessFunction[String, Row, String]{
//
//        //广播中放置的数据容器
//        val accBCValues :java.util.HashMap[String,Object] = new util.HashMap[String,Object]();
//
//        /**
//          * 处理广播中的数据然后发送到下一环节处理
//          * @param value 广播数据记录
//          * @param ctx 广播处理函数
//          * @param out 输出
//          */
//        override def processBroadcastElement(value: Row, ctx: BroadcastProcessFunction[String, Row, String]#Context, out: Collector[String]): Unit = {
//          import org.apache.flink.api.common.state.BroadcastState
//          val broadcastState : BroadcastState[String,String] = ctx.getBroadcastState(dimMapStateDesc)
//
//          accBCValues.putAll(value)
//          broadcastState.put("keyWords", keyWords)
//        }
//
//        override def processElement(value: String, ctx: BroadcastProcessFunction[String, Row, String]#ReadOnlyContext, out: Collector[String]): Unit = {
//
//        }
//
//        override def open(parameters: Configuration): Unit = super.open(parameters)
//
//        override def close(): Unit = super.close()
//      }
//    );
//      midDataStream
//  }



  /**
    * 实时统计
    * 事实数据与维表数据形成宽表数据
    */
  def handleStreamingWideETLJob(appName:String, fromTopic:String, toTopic:String):Unit = {

    //1 flink环境初始化
    val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
    //使用事件时间做处理参考
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //2 维表数据集
    val dimDStream :DataStream[Row] = readActionDimDataStream(env)
    dimDStream.print()


    //3 kafka流式数据源
    val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, consumerProperties)
    kafkaConsumer.setStartFromLatest()

    //4 实时流数据集合操作
    val dStream :DataStream[String] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
    val mStream :DataStream[String] = travelDataMap(dStream)
    val fmStream:DataStream[String] = travelDataFlatMap(mStream)

    //5 事实数据与维表数据关联



    //5 写入下游环节
    //    val kafkaSerSchema = new QkafkaSerializationSchema(toTopic)
    //    val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
    //    val travelKafkaProducer = new FlinkKafkaProducer(
    //        toTopic,
    //        kafkaSerSchema,
    //        kafkaProductConfig,
    //        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
    //
    //    //6 加入kafka摄入时间
    //    travelKafkaProducer.setWriteTimestampToKafka(true)
    //    fmStream.addSink(travelKafkaProducer)
    //fmStream.print()

    env.execute(appName)
  }










  def main(args: Array[String]): Unit = {
    //参数处理
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val appName = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_APPNAME)
    //    val fromTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_FROM)
    //    val toTopic = parameterTool.get(QRealTimeConstant.PARAMS_KEYS_TOPIC_TO)

    val appName = "qf.realtime.etl.QRealtimeWideEtl"
    val fromTopic = QRealTimeConstant.TOPIC_FROM
    val toTopic = QRealTimeConstant.TOPIC_TO_WIDE


    //实时处理第一层：ETL
    handleStreamingWideETLJob(appName, fromTopic, toTopic)



  }

}
