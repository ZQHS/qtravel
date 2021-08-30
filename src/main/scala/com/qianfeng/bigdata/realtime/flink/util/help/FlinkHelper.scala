package com.qianfeng.bigdata.realtime.flink.util.help

import java.util.Properties

import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qianfeng.bigdata.realtime.flink.streaming.assinger.OrdersPeriodicAssigner
import com.qianfeng.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.OrderDetailDataMapFun
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.OrderDetailData
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.QRealTimeDimDO.ProductDimDO
import com.qianfeng.bigdata.realtime.flink.streaming.rdo.typeinformation.QRealTimeDimTypeInformation
import com.qianfeng.bigdata.realtime.flink.util.es.ESConfigUtil
import com.qianfeng.bigdata.realtime.flink.util.es.ESConfigUtil.ESConfigHttpHost
import com.qianfeng.bigdata.realtime.util.PropertyUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.config.StartupMode
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._

/**
 * flink的相关工具包
 * 1、获取flink的流式执行环境
 * 2、获取flink-kafka的消费者
 * 3、获取产品的维度的数据
 * 4、获取redis的连接
 */
object FlinkHelper {

  //日志打印对象
  private val logger: Logger = LoggerFactory.getLogger("flinkHelper")

  /*
  获取flink的流式执行环境
   */
  def createStreamingEnvironment(checkpointInterval:Long,
                                 tc:TimeCharacteristic,
                                 watermarkInterval:Long):StreamExecutionEnvironment = {
    var env:StreamExecutionEnvironment = null
    //获取流式执行环境
    try {
      env = StreamExecutionEnvironment.getExecutionEnvironment
      //设置并行度
      env.setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      //开启checkpoint
      env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE)
      //设置任务失败重启机制
      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
        QRealTimeConstant.RESTART_ATTEMPTS,
        Time.seconds(10)))

      //设置env的时间类型
      env.setStreamTimeCharacteristic(tc)
      //设置水位间隔时间
      env.getConfig.setAutoWatermarkInterval(watermarkInterval)
    } catch {
      case e:Exception => e.printStackTrace()
    }
    //返回执行环境
    env
  }

  /**
   * 使用默认的间隔和时间语义来获取执行环境
   * @return
   */
  def createStreamingEnvironment():StreamExecutionEnvironment = {
    val env:StreamExecutionEnvironment = createStreamingEnvironment(
      QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL,
      TimeCharacteristic.EventTime,
      QRealTimeConstant.FLINK_WATERMARK_INTERVAL
    )
    //返回执行环境
    env
  }

  /**
   * 返回flink-kafka的消费者
   * @param env
   * @param topic
   * @param groupID
   * @return
   */
  def createKafkaConsumer(env:StreamExecutionEnvironment,topic:String,groupID:String):FlinkKafkaConsumer[String] = {
    //kafka的反序列化
    val schema = new SimpleStringSchema()

    //需要kafka的相关参数
    val pro: Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    //重置消费者组
    pro.setProperty("group.id",groupID)

    //创建kafka的消费者
    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer(topic, schema, pro)

    //设置自动提交offset
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    //返回
    kafkaConsumer
  }

  /**
   * 返回flink-kafka的消费者
   * @param env
   * @param topic
   * @param groupID
   * @param schema
   * @param sm
   * @tparam T
   * @return
   */
  def createKafkaSerDeConsumer[T:TypeInformation](env:StreamExecutionEnvironment, topic:String, groupID:String,
                                             schema:KafkaDeserializationSchema[T], sm:StartupMode):FlinkKafkaConsumer[T] = {
    //需要kafka的相关参数
    val pro: Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    //重置消费者组
    pro.setProperty("group.id",groupID)

    //创建kafka的消费者
    val kafkaConsumer: FlinkKafkaConsumer[T] = new FlinkKafkaConsumer(topic,schema,pro)
    //设置kafka的消费位置
    if(sm.equals(StartupMode.EARLIEST)){
      kafkaConsumer.setStartFromEarliest()
    } else if(sm.equals(StartupMode.LATEST)){
      kafkaConsumer.setStartFromLatest()
    }
    //设置自动提交offset
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    //返回
    kafkaConsumer
  }

  /**
   * 创建产品维表数据流
   * @param env flink上下文对象
   * @param sql 查询sql
   * @param fieldTypes 查询对应的列字段
   * @return
   */
  def createProductDimDStream(env: StreamExecutionEnvironment, sql:String,
                              fieldTypes: Seq[TypeInformation[_]]): DataStream[ProductDimDO] = {
    FlinkHelper.createOffLineDataStream(env, sql, fieldTypes).map(
      row => {
        val productID = row.getField(0).toString
        val productLevel = row.getField(1).toString.toInt
        val productType = row.getField(2).toString
        val depCode = row.getField(3).toString
        val desCode = row.getField(4).toString
        val toursimType = row.getField(5).toString
        ProductDimDO(productID, productLevel, productType, depCode, desCode, toursimType)
      }
    )
  }

  /**
   * 创建jdbc数据源输入格式
   * @param driver jdbc连接驱动
   * @param username jdbc连接用户名
   * @param passwd jdbc连接密码
   * @return
   */
  def createJDBCInputFormat(driver:String, url:String, username:String, passwd:String,
                            sql:String, fieldTypes: Seq[TypeInformation[_]]): JDBCInputFormat = {

    //sql查询语句对应字段类型列表
    val rowTypeInfo = new RowTypeInfo(fieldTypes:_*)

    //数据源提取
    val jdbcInputFormat :JDBCInputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(driver)
      .setDBUrl(url)
      .setUsername(username)
      .setPassword(passwd)
      .setRowTypeInfo(rowTypeInfo)
      .setQuery(sql)
      .finish()
    //返回jdbc的输入格式
    jdbcInputFormat
  }


  /**
   * 维度数据加载
   * @param env flink上下文环境对象
   * @param sql 查询语句
   * @param fieldTypes 查询语句对应字段类型列表
   * @return
   */
  def createOffLineDataStream(env: StreamExecutionEnvironment,
                              sql:String,
                              fieldTypes: Seq[TypeInformation[_]]):DataStream[Row] = {
    //JDBC属性
    val mysqlDBProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.MYSQL_CONFIG_URL)

    //flink-jdbc包支持的jdbc输入格式
    val jdbcInputFormat : JDBCInputFormat= FlinkHelper.createJDBCInputFormat(mysqlDBProperties, sql, fieldTypes)

    //jdbc数据读取后形成数据流[其中Row为数据类型，类似jdbc]
    //flink-core包中的
    val jdbcDataStream :DataStream[Row] = env.createInput(jdbcInputFormat)
    //数据流
    jdbcDataStream
  }


  /**
   * 创建jdbc数据源输入格式
   * @param properties jdbc连接参数
   * @param sql 查询语句
   * @param fieldTypes 查询语句对应字段类型列表
   * @return
   */
  def createJDBCInputFormat(properties:Properties, sql:String, fieldTypes: Seq[TypeInformation[_]]): JDBCInputFormat = {
    //jdbc连接参数
    val driver :String = properties.getProperty(QRealTimeConstant.FLINK_JDBC_DRIVER_MYSQL_KEY)
    val url :String = properties.getProperty(QRealTimeConstant.FLINK_JDBC_URL_KEY)
    val user:String = properties.getProperty(QRealTimeConstant.FLINK_JDBC_USERNAME_KEY)
    val passwd:String = properties.getProperty(QRealTimeConstant.FLINK_JDBC_PASSWD_KEY)

    //flink-jdbc包支持的jdbc输入格式
    val jdbcInputFormat : JDBCInputFormat = createJDBCInputFormat(driver, url, user, passwd,
      sql, fieldTypes)

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
   * redis连接参数(单点)
   */
  def createRedisConfig(db:Int) : FlinkJedisConfigBase = {

    //redis配置文件
    val redisProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.REDIS_CONFIG_PATH)

    //redis连接参数
    var redisDB :Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_DB).toInt
    if(null != db){
      redisDB = db
    }

    val redisMaxIdle :Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_MAXIDLE).toInt
    val redisMinIdle:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_MINIDLE).toInt
    val redisMaxTotal:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_MAXTOTAL).toInt
    val redisHost:String = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_HOST)
    val redisPassword:String = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_PASSWORD)
    val redisPort:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_PORT).toInt
    val redisTimeout:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_TIMEOUT).toInt

    //redis配置对象构造
    // 单机连接
    new FlinkJedisPoolConfig.Builder()
      .setHost(redisHost)
      .setPort(redisPort)
      .setPassword(redisPassword)
      .setTimeout(redisTimeout)
      .setDatabase(redisDB)
      .setMaxIdle(redisMaxIdle)
      .setMinIdle(redisMinIdle)
      .setMaxTotal(redisMaxTotal)
      .build
  }

  /**
   * 旅游产品实时明细数据流
   * @param groupID 消费分组
   * @param fromTopic kafka消费主题
   * @return
   */
  def createOrderDetailDStream(env: StreamExecutionEnvironment,
                               groupID:String,
                               fromTopic:String,
                               timeChar :TimeCharacteristic):DataStream[OrderDetailData] ={
    /**
     * kafka流式数据源
     * kafka消费配置参数
     * kafka消费策略
     */
    val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, groupID)

    /**
     * 旅游产品订单数据
     *   (1) kafka数据源(原始明细数据)->转换操作
     *   (2) 设置执行任务并行度
     *   (3) 设置水位及事件时间(如果时间语义为事件时间)
     */
    //固定范围的水位指定(注意时间单位)
    var orderDetailDStream :DataStream[OrderDetailData] = env.addSource(kafkaConsumer)
      .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      .map(new OrderDetailDataMapFun())

    if(TimeCharacteristic.EventTime.equals(timeChar)){
      // 保证数据不乱序，延迟5秒
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      orderDetailDStream  = orderDetailDStream.assignTimestampsAndWatermarks(ordersPeriodicAssigner)
    }
    orderDetailDStream
  }

  def main(args: Array[String]): Unit = {

   /* //测试flink的执行环境变量
    println(createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL,
      TimeCharacteristic.EventTime,QRealTimeConstant.FLINK_WATERMARK_INTERVAL))

      //测试重载方法
      print(createStreamingEnvironment())*/


    val env: StreamExecutionEnvironment = createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL,
      TimeCharacteristic.EventTime, QRealTimeConstant.FLINK_WATERMARK_INTERVAL)

    //测试flink-jdbc的连接器
//    val res: DataStream[Row] = createOffLineDataStream(
//      env,
//      QRealTimeConstant.SQL_PRODUCT,
//      QRealTimeDimTypeInformation.getProductDimFieldTypeInfos()
//    )
//    res.print("flink-jdbc的结果集->")

    //测试获取es集群的http的配置
    println(getESCluster())

    //测试获取redis的配置对象
//    println(createRedisConfig(0))


    //kafka的消费测试
    //java -jar ./target/qdata-1.0-TravelCurLogJob.jar  --topic travel_logs_ods --source ods    --count 10  --sleep 3000
//    val res: DataStream[String] = env.addSource(createKafkaConsumer(
//      env, "travel_logs_ods", "logs-group-id"))
//    res.print("flink-kafka数据->")


//    val orderDetailDStream: DataStream[OrderDetailData] = createOrderDetailDStream(
//      env, "order-group-id", "t_travel_ods", TimeCharacteristic.EventTime)
//    orderDetailDStream.print("订单明细数据->")

    //触发执行
//    env.execute("product")
  }
}
