package com.qf.bigdata.realtime.flink.util.help

import java.util.Properties

import com.qf.bigdata.realtime.constant.TravelConstant
import com.qf.bigdata.realtime.flink.batch.ReleaseBatchJob.LOG
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.util.es.ESConfigUtil
import com.qf.bigdata.realtime.flink.util.es.ESConfigUtil.ESConfigHttpHost
import com.qf.bigdata.realtime.util.json.{JsonMapperUtil, JsonUtil}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.slf4j.{Logger, LoggerFactory}

object FlinkHelper {

  val logger :Logger = LoggerFactory.getLogger("FlinkHelper")

  //默认重启次数
  val DEF_FLINK_RESTART_STRATEGY_NUM = 10


  /**
    * 流式环境下的flink上下文构建
    * @param appName
    */
  def createStreamingEnvironment(checkPointInterval :Long) :StreamExecutionEnvironment = {
    var env : StreamExecutionEnvironment = null
    try{
      //构建flink批处理上下文对象
      env = StreamExecutionEnvironment.getExecutionEnvironment

      //设置执行并行度
      env.setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)

      //开启checkpoint
      env.enableCheckpointing(checkPointInterval, CheckpointingMode.EXACTLY_ONCE)

      //flink服务重启机制
      //env.setRestartStrategy()

      //flink
      //senv.setStateBackend()

    }catch{
      case ex:Exception => {
        println(s"FlinkHelper create flink context occur exception：msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }
    env
  }


  /**
    * flink读取kafka数据
    * @param env
    * @param topic
    * @param properties
    * @return
    */
  def createKafkaConsumer(env:StreamExecutionEnvironment, topic:String, properties:Properties) :FlinkKafkaConsumer[String] = {
    //kafka数据序列化
    val schema = new SimpleStringSchema()

    //创建消费者和消费策略
    val kafkaConsumer : FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic, schema, properties)


    kafkaConsumer
  }



  /**
    * 参数处理工具
    * @param args
    * @return
    */
  def createParameterTool(args: Array[String]):ParameterTool = {
    val parameterTool = ParameterTool.fromArgs(args)
    parameterTool
  }


  /**
    * 创建jdbc数据源输入格式
    * @param driver
    * @param username
    * @param passwd
    * @return
    */
  def createJDBCInputFormat(driver:String, url:String, username:String, passwd:String,
                            query:String, fieldTypes: Seq[TypeInformation[_]]): JDBCInputFormat = {

    //记录列信息
    val rowTypeInfo = new RowTypeInfo(fieldTypes:_*)

    //数据源提取
    val jdbcInputFormat :JDBCInputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(driver)
      .setDBUrl(url)
      .setUsername(username)
      .setPassword(passwd)
      .setRowTypeInfo(rowTypeInfo)
      .setQuery(query)
      .finish();

    jdbcInputFormat
  }


  /**
    * 创建jdbc数据源输入格式
    * @param properties
    * @param query
    * @param fieldTypes
    * @return
    */
  def createJDBCInputFormat(properties:Properties, query:String, fieldTypes: Seq[TypeInformation[_]]): JDBCInputFormat = {
    val driver :String = properties.getProperty(TravelConstant.FLINK_JDBC_DRIVER_MYSQL_KEY)
    val url :String = properties.getProperty(TravelConstant.FLINK_JDBC_URL_KEY)
    val user:String = properties.getProperty(TravelConstant.FLINK_JDBC_USERNAME_KEY)
    val passwd:String = properties.getProperty(TravelConstant.FLINK_JDBC_PASSWD_KEY)

    val jdbcInputFormat : JDBCInputFormat = createJDBCInputFormat(driver, url, user, passwd,
      query, fieldTypes)
    jdbcInputFormat
  }


  /**
    * ES集群地址
    * @return
    */
  def getESCluster() : ESConfigHttpHost = {
    ESConfigUtil.getConfigHttpHost(QRealTimeConstant.ES_CONFIG_PATH)
  }


  /**
    * ES Sink输出(与版本有关)
    * @param index
    * @return
    */
//  def createESSink(index:String) : ElasticsearchSink[String] =  {
//    //ES集群
//    val esConfig :ESConfigHttpHost = getESCluster()
//    val httpHosts :java.util.ArrayList[HttpHost] = esConfig.transportAddresses
//
//    //ES输出数据处理函数
//    val esSinkFunction = new ElasticsearchSinkFunction[String]{
//
//      import org.elasticsearch.action.index.IndexRequest
//      import org.elasticsearch.client.Requests
//
//      //创建索引
//      def createIndexRequest(element: String): IndexRequest = {
//        //ES6 source函数要求参数为偶数即Map结构
//        //val elementMap :java.util.Map[String,String] = JsonUtil.json2object(element, classOf[java.util.Map[String,String]])
//
//        val elementMap :java.util.Map[String,String] = JsonMapperUtil.readValue(element, classOf[java.util.Map[String,String]])
//
//        Requests.indexRequest.index(index).create(true)
//           .`type`(index).source(elementMap)
//      }
//
//      //向索引添加数据
//      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
//        val indexedReq = createIndexRequest(element)
//        indexer.add(indexedReq)
//      }
//    }
//    val esSinkBuilder = new ElasticsearchSink.Builder[String](httpHosts,esSinkFunction)
//    esSinkBuilder.build()
//  }


}
