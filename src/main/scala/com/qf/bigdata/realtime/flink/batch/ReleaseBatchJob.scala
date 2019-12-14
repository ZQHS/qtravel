package com.qf.bigdata.realtime.flink.batch

import com.qf.bigdata.realtime.bean.{ReleaseBean, SourcesBean}
import com.qf.bigdata.realtime.constant.{CommonConstant, TravelConstant}
import com.qf.bigdata.realtime.enumes.SourceEnum
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.functions.QFunctions.{QFContainFilterFun, QFReleaseMapFun, ToCaseClassMapFun}
import com.qf.bigdata.realtime.flink.util.help.MetaDataHelper
import com.qf.bigdata.realtime.util.{CommonUtil}
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment

import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.{Logger, LoggerFactory}

/**
  * 广告投放数据实时采集落地
  */

class ReleaseBatchJob{

}

object ReleaseBatchJob {

  val LOG :Logger = LoggerFactory.getLogger(ReleaseBatchJob.getClass)



  //json解析
  val objectMapper: ObjectMapper = new ObjectMapper()




  /**
    * 简版投放数据
    * @param sources
    * @param channels
    * @param mediaType
    * @param day
    */
  case class MinRelease(sources:String,channels:String, mediaType:Long, bdp_day:String)


  case class ReleaseSource(sources:String, channels:String, mediaType:Long,
                           device_num:String, device_type:String,
                           release_session:String, release_status:String, exts:String,
                           bdp_day:String)

  case class MinReleaseSource(sources:String, channels:String, mediaType:Long,
                           device_num:String, device_type:String,
                           release_session:String, release_status:String,
                              bdp_day:String)

  case class AggReleaseSource(bdp_day:String, release_status:String,sources:String, channels:String, mediaType:Long,
                            release_count:Long, release_dist:Long)



  //===demo===============================================================================

  /**
    * flink处理流式数据写入HDFS
    * @param appName
    */
  def handleBatchData4CSVBroadcast(appName :String) :Unit = {
    //渠道通道维表数据(文件形式)
    val scDimPath:String = TravelConstant.HDFS_SOURCES_MAPPING_CHANNELS
    //广告投放数据
    val releaseOdsPath:String = TravelConstant.HDFS_RELEASE_ODS

    //引号设置(csv里的引号，默认列分割符号[,]会影响列的读取)
    val quato :Character = TravelConstant.DEF_QUOTE
    val fieldDelimiter :String = TravelConstant.DEF_FIELD_DELIMITER
    val brSource :String = TravelConstant.BROADCAST_SOURCE

    try{
      //构建flink批处理上下文对象
      //val env = ExecutionEnvironment.getExecutionEnvironment
      val env = ExecutionEnvironment.createLocalEnvironment(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      //设置执行并行度
      //env.setParallelism(FReleaseConstant.DEF_LOCAL_PARALLELISM)

      //csv数据源,使用pojo的字段
      //渠道维表数据
      val sourceDSFields :Array[String] = MetaDataHelper.getSourceChannelColumns()
      val sourceDS :DataSet[SourcesBean] = env.readCsvFile[SourcesBean](scDimPath, quoteCharacter=quato,ignoreFirstLine=true, pojoFields = sourceDSFields)

      //投放数据
      val releaseDSFields :Array[String] = MetaDataHelper.getReleaseColumns()
      val releaseDS :DataSet[ReleaseBean] = env.readCsvFile[ReleaseBean](releaseOdsPath,fieldDelimiter = fieldDelimiter, quoteCharacter=quato,ignoreFirstLine=true, pojoFields = releaseDSFields)

      //关联后的新数据集
      val releaseSourceDS = releaseDS.map(new QFReleaseMapFun(brSource)).withBroadcastSet(sourceDS, brSource)

      println("===releaseSourceDS===" * 10)
      releaseSourceDS.print()


    }catch{
      case ex:Exception => {
        println(s"flink.ReleaseBatchJob occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }finally {

    }
  }

  /**
    * flink处理流式数据写入HDFS
    * @param appName
    */
  def handleBatchData4CSVRecursion(appName :String) :Unit = {
    //渠道通道维表数据(文件形式)
    val sourcePath:String = TravelConstant.HDFS_SOURCES_MAPPING_CHANNELS_RECURSION
    try{
      //构建flink批处理上下文对象
      //val env = ExecutionEnvironment.getExecutionEnvironment
      val env = ExecutionEnvironment.createLocalEnvironment()
      //设置执行并行度
      env.setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)

      //csv数据源
      val ds:DataSet[(String,String,String,String,Long,String,Long)] =
        env.readCsvFile[(String,String,String,String,Long,String,Long)](sourcePath,ignoreFirstLine=true)

      //设置递归参数
      val parameters = new Configuration()
      parameters.setBoolean("recursive.file.enumeration",true)
      val reDS = ds.withParameters(parameters)

      println("===df===" * 10)
      reDS.print()

    }catch{
      case ex:Exception => {
        println(s"flink.ReleaseBatchJob occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }
  }

  /**
    * flink处理流式数据写入HDFS
    * @param appName
    */
  def handleBatchData4CSVRichFun(appName :String) :Unit = {
    //渠道通道维表数据(文件形式)
    val sourcePath:String = TravelConstant.HDFS_SOURCES_MAPPING_CHANNELS_RECURSION
    try{
      //构建flink批处理上下文对象
      //val env = ExecutionEnvironment.getExecutionEnvironment
      val env = ExecutionEnvironment.createLocalEnvironment()
      //设置执行并行度
      env.setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)

      //csv数据源
      val ds:DataSet[(String,String,String,String,Long,String,Long)] =
        env.readCsvFile[(String,String,String,String,Long,String,Long)](sourcePath,ignoreFirstLine=true)

      //设置递归参数
      val parameters = new Configuration()
      parameters.setBoolean("recursive.file.enumeration",true)
      val reDS = ds.withParameters(parameters)
      println("===df===" * 10)
      reDS.print()

      //过滤
      val ranges = "topline,douyin"
      val split = ","
      val filterDS:DataSet[(String,String,String,String,Long,String,Long)] = reDS.filter(new QFContainFilterFun(ranges, split))
      println("===filterDS===" * 10)
      filterDS.print()

    }catch{
      case ex:Exception => {
        println(s"flink.ReleaseBatchJob occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }
  }


  /**
    * flink处理流式数据写入HDFS
    * @param appName
    */
  def handleBatchData4CSV(appName :String) :Unit = {
    //渠道通道维表数据(文件形式)
    val sourcePath:String = TravelConstant.HDFS_SOURCES_MAPPING_CHANNELS_DUL
    try{
      //构建flink批处理上下文对象
      //val env = ExecutionEnvironment.getExecutionEnvironment
      val env = ExecutionEnvironment.createLocalEnvironment(QRealTimeConstant.DEF_LOCAL_PARALLELISM)

      //csv数据源
      val df:DataSet[(String,String,String,String,Long,String,Long)] =
        env.readCsvFile[(String,String,String,String,Long,String,Long)](sourcePath,ignoreFirstLine=true)

      println("===df===" * 10)
      df.print()

      //转换去重
      val distDF = df.map(new ToCaseClassMapFun())
                     .distinct(TravelConstant.SOURCES)
      println("===distDF===" * 10)
      distDF.print()

      //过滤数据集
      //自定义MapFunction函数
      val filterDF = distDF.filter(_.sources.equalsIgnoreCase(SourceEnum.BAIDU.getCode))
      println("===filterDF===" * 10)
      filterDF.print()

      //单列聚合操作
      val aggDF = distDF.aggregate(Aggregations.SUM,"mediaType").andMax("mediaType")
      println("===aggDF===" * 10)
      aggDF.print()

      //分组聚合
      val groupDF = distDF.groupBy("day").sum("mediaType")
          .andMax("mediaType").andMin("sources")
      println("===groupDF===" * 10)
      groupDF.print()

      //输出结果
      //明细输出
      distDF.writeAsCsv(TravelConstant.HDFS_SOURCES_MAPPING_CHANNELS_CSV_OUT, writeMode=WriteMode.OVERWRITE)
      //聚合结果输出
      groupDF.writeAsCsv(TravelConstant.HDFS_SOURCES_MAPPING_CHANNELS_CSV_AGG_OUT, writeMode=WriteMode.OVERWRITE)

      //设置执行并行度
      env.setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)

      //任务lazy处理触发
      env.execute(appName)

    }catch{
      case ex:Exception => {
        println(s"flink.ReleaseBatchJob occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }
  }


  /**
    * flink处理流式数据写入HDFS
    * @param appName
    */
  def handleBatchData4CSVPojo(appName :String) :Unit = {
    //渠道通道维表数据(文件形式)
    val scDimPath:String = TravelConstant.HDFS_SOURCES_MAPPING_CHANNELS
    //广告投放数据
    val releaseOdsPath:String = TravelConstant.HDFS_RELEASE_ODS

    //引号设置(csv里的引号，默认列分割符号[,]会影响列的读取)
    val quato :Character = TravelConstant.DEF_QUOTE
    val fieldDelimiter :String = TravelConstant.DEF_FIELD_DELIMITER

    try{
      //构建flink批处理上下文对象
      //val env = ExecutionEnvironment.getExecutionEnvironment
      val env = ExecutionEnvironment.createLocalEnvironment(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      //flink自带序列化 VS kryo序列化 VS Avro序列化
      //env.getConfig.enableForceKryo()
      //env.getConfig.enableForceAvro()

      //env.registerType(classOf[ReleaseBean])
      //env.registerType(classOf[SourcesBean])
      //env.getConfig.registerPojoType(classOf[ReleaseBean])
      //env.getConfig.registerPojoType(classOf[SourcesBean])


      //csv数据源
      //使用pojo的字段
      val releaseDSFields :Array[String] = MetaDataHelper.getReleaseColumns()
      val sourceDSFields :Array[String] = MetaDataHelper.getSourceChannelColumns()

      val releaseDS :DataSet[ReleaseBean] = env.readCsvFile[ReleaseBean](releaseOdsPath,fieldDelimiter = fieldDelimiter, quoteCharacter=quato,ignoreFirstLine=true, pojoFields = releaseDSFields)
      val sourceDS :DataSet[SourcesBean] = env.readCsvFile[SourcesBean](scDimPath, quoteCharacter=quato,ignoreFirstLine=true, pojoFields = sourceDSFields)

      //数据集关联
      val joinDS :DataSet[ReleaseSource] = releaseDS.join(sourceDS, JoinHint.BROADCAST_HASH_SECOND)
          .where("sources")
          .equalTo("sources") {
            (r:ReleaseBean, s:SourcesBean) => {
              val sources = r.getSources
              val channels = r.getChannels
              val mediaType = s.getMedia_type.toInt
              val deviceNum = r.getDevice_num
              val deviceType = r.getDevice_type
              val releaseSession = r.getRelease_session
              val releaseStatus = r.getRelease_status
              val exts = r.getExts
              val day = CommonUtil.formatDate4Timestamp(r.getCt, TravelConstant.DEF_FORMAT)
              ReleaseSource(sources, channels, mediaType, deviceNum, deviceType, releaseSession, releaseStatus, exts, day)
            }
          }
      println("===joinDS===" * 10)
      joinDS.print()
      joinDS.writeAsCsv(TravelConstant.HDFS_RELEASE_SOURCES_OUT, fieldDelimiter = fieldDelimiter, writeMode=WriteMode.OVERWRITE)


      //设置执行并行度
      env.setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)

      //任务lazy处理触发
      env.execute(appName)


    }catch{
      case ex:Exception => {
        println(s"flink.ReleaseBatchJob occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }
  }

  /**
    * flink处理流式数据写入HDFS
    * @param appName
    */
  def handleBatchData4Table(appName :String) :Unit = {
    //渠道通道维表数据(文件形式)
    val scDimPath:String = TravelConstant.HDFS_SOURCES_MAPPING_CHANNELS
    //广告投放数据
    val releaseOdsPath:String = TravelConstant.HDFS_RELEASE_ODS

    //引号设置(csv里的引号，默认列分割符号[,]会影响列的读取)
    val quato :Character = TravelConstant.DEF_QUOTE
    val fieldDelimiter :String = TravelConstant.DEF_FIELD_DELIMITER

    try{
      //构建flink批处理上下文对象
      val env = ExecutionEnvironment.getExecutionEnvironment

      //设置执行并行度
      env.setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)

      //二维表结构处理
      val tableEnv = TableEnvironment.getTableEnvironment(env)

      //csv数据源
      //使用pojo的字段
      val releaseDSFields :Array[String] = MetaDataHelper.getReleaseColumns()
      val sourceDSFields :Array[String] = MetaDataHelper.getSourceChannelColumns()

      val releaseDS :DataSet[ReleaseBean] = env.readCsvFile[ReleaseBean](releaseOdsPath,fieldDelimiter = fieldDelimiter, quoteCharacter=quato,ignoreFirstLine=true, pojoFields = releaseDSFields)
      val sourceDS :DataSet[SourcesBean] = env.readCsvFile[SourcesBean](scDimPath, quoteCharacter=quato,ignoreFirstLine=true, pojoFields = sourceDSFields)

      //数据集关联
      val joinDS :DataSet[ReleaseSource] = releaseDS.join(sourceDS, JoinHint.BROADCAST_HASH_SECOND)
        .where("sources")
        .equalTo("sources") {
          (r:ReleaseBean, s:SourcesBean) => {
            val sources = r.getSources
            val channels = r.getChannels
            val mediaType = s.getMedia_type.toInt
            val deviceNum = r.getDevice_num
            val deviceType = r.getDevice_type
            val releaseSession = r.getRelease_session
            val releaseStatus = r.getRelease_status
            val exts = r.getExts
            val day = CommonUtil.formatDate4Timestamp(r.getCt, TravelConstant.DEF_FORMAT)
            ReleaseSource(sources, channels, mediaType, deviceNum, deviceType, releaseSession, releaseStatus, exts, day)
          }
        }
      println("===joinDS===" * 10)
      joinDS.print()

      //元数据schema
      val releaseSourceColumns :Seq[String] = MetaDataHelper.getReleaseSourceColumns()
      val releaseExpress: List[Expression] = ExpressionParser.parseExpressionList(releaseSourceColumns.mkString(CommonConstant.COMMA));

      //注册数据集成表
      tableEnv.registerDataSet[ReleaseSource](TravelConstant.TABLE_RELEASE_SOURCE, joinDS, releaseExpress:_*)
      val sql =
        s"""
           |select
           |  bdp_day, release_status, sources, channels, mediaType,
           |  count(*) as release_count,
           |  count(distinct release_session) as release_dist
           |from ${TravelConstant.TABLE_RELEASE_SOURCE}
           |group by
           |  bdp_day, release_status, sources, channels, mediaType
         """.stripMargin

      //投放数据统计表(flink table)
      val table = tableEnv.sqlQuery(sql)
      val aggReleaseSourceTable:DataSet[AggReleaseSource] = tableEnv.toDataSet[AggReleaseSource](table)
      aggReleaseSourceTable.print()


    }catch{
      case ex:Exception => {
        println(s"flink.ReleaseBatchJob occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }
  }








  def main(args: Array[String]): Unit = {

    //val Array(appName, groupID, topic, output) = args

    val appName: String = "qf_flink"

    val begin = System.currentTimeMillis()

    //单一文件处理
    //handleBatchData4CSV(appName)
    //handleBatchData4CSVRecursion(appName)
    //handleBatchData4CSVRichFun(appName)
    //handleBatchData4CSVBroadcast(appName)
    handleBatchData4Table(appName)



    //多文件处理
    //handleBatchData4CSVPojo(appName)

    val end = System.currentTimeMillis()

    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }



}
