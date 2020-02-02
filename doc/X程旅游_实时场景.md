# X程旅游_实时场景

## 第一章 行业特征

* 用户数量：近亿级

```
【携程】  亿级 
【去哪儿】 亿级
【飞猪】   千万级
【马蜂窝】 千万级 
【途牛】   千万级
```

活跃用户：千万级

```
【携程】  千万级(5588万) 
【去哪儿】 千万级(4133万)
【飞猪】   千万级(2888万)
【马蜂窝】 千万级(2294万)
【途牛】   近千万级(944万)
```



* 数据量级

​	 请求级别：亿级 每天TB级的增量数据，近百亿条的用户数据，上百万的产品数据



* 集群规模

  万台+ | 千台+

  

* 生活娱乐类服务场景APP

  用户交易订单不会高频率呈现，但会集中于个别时间节点(如公共假期、周末、寒暑假等)

  旅游行业作为综合性产业覆盖了：住宿、餐饮、购物、交通等其他相关行业，多元化结合

  用户交互信息数量巨大，而且会涉及其他社交类APP的使用



* 核心业务介绍

```
1 旅游产品业务
2 相关业务：车票、酒店等紧密关系业务
3 用户交互
	3.1 用户行为
		* 评论
		* 点赞
		* 分享、转发
		* 收藏
		* 关注
	3.2 用户与用户
		* 关注、推荐、邀请（关系：好友、粉丝）
		* 圈子
4 消息
	4.1 系统推荐（活动、推广）
	4.2 用户间消息
	
5 画像
	用户群体画像
	用户画像

6 运营
	* 用户层次定位、用户构成
	* 用户粘性：DAU|MAU
	* 各种离线、实时统计指标
	* 各种业务的实时推荐
```



## 第二章 大数据架构(实时方向)

![](pic\realtime_olap.png)



```
数据来源：
	实时消息流 Kafka
计算：
	实时计算 Flink
资源管理
	Kubernetes
	Yarn
实时数据存储(主要用于客户端的交互式查询，包括聚合、明细、搜索)
	时序库
		apache Druid
	NOSql(实时采集或离线采集后进行对数校验任务)
		hive
		hbase
	图数据库(实时数据处理中形成图关系但在并发和计算较重任务中不建议使用而放在离线处理)
		Neo4j
		JanusGraph
缓存方案
	redis
	levelDB -> RocksDB

```



## 第三章 业务处理

### 开发准备工作

由于pom文件较长，目前仅贴出后续要使用的依赖包，具体情况可参考项目代码

```xml
<!-- mysql -->
<dependency>
	<groupId>mysql</groupId>
	<artifactId>mysql-connector-java</artifactId>
	<version>5.1.44</version>
</dependency>

<!-- kafka-->
<dependency>
	<groupId>org.apache.kafka</groupId>
	<artifactId>kafka_2.11</artifactId>
	<version>1.1.1</version>
</dependency>

<dependency>
	<groupId>org.apache.kafka</groupId>
	<artifactId>kafka-clients</artifactId>
	<version>1.1.1</version>
</dependency>

<!-- flink -->
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-core</artifactId>
	<version>1.9.1</version>
</dependency>

<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-scala_2.11</artifactId>
	<version>1.9.1</version>
</dependency>

<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-streaming-scala_2.11</artifactId>
	<version>1.9.1</version>
</dependency>

<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-clients_2.11</artifactId>
	<version>1.9.1</version>
</dependency>

<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-connector-kafka_2.11</artifactId>
	<version>1.9.1</version>
</dependency>

<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-table_2.11</artifactId>
	<version>${flink.table.version}</version>
</dependency>

<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-jdbc_2.11</artifactId>
	<version>1.9.1</version>
</dependency>
```



### 第一节 实时计算上下文环境构建

#### 背景说明

```
对于使用Flink框架来处理实时场景来说，首先是要构建的是StreamExecutionEnvironment，当然Flink也提供了处理离线场景的对应上下文对象ExecutionEnvironment
基于功能复用的思想我们封装了一些常用的功能形成Flink帮助类：FlinkHelper，下面这段代码提取了构建上下文环境对象的函数。
```



##### 构建实时上下文环境对象

```scala
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
  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(QRealTimeConstant.RESTART_ATTEMPTS,QRealTimeConstant.RESTART_DELAY_BETWEEN_ATTEMPTS))


    }catch{
      case ex:Exception => {
        println(s"FlinkHelper create flink context occur exception：msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }
    env
  }
```



### 第二节 数据通道

#### 背景说明

```
目前实时场景的数据来源一般是取自消息队列(如kafka)，下面我们看看如何使用Flink来进行kafka数据的读取工作。
我们以用户行为日志中的点击行为为示例进行说明，下例为用户点击行为明细处理类UserLogsClickHandler中的局部片段，具体完整代码请参考项目相关代码。
```



##### 点击行为日志数据处理

```scala
//1 flink环境初始化使用事件时间做处理参考
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.getConfig.setAutoWatermarkInterval(QRealTimeConstant.FLINK_WATERMARK_INTERVAL)

//2 kafka流式数据源
      val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)

//创建消费者和消费策略
      val schema:KafkaDeserializationSchema[UserLogData] = new UserLogsKSchema(fromTopic)
      val kafkaConsumer : FlinkKafkaConsumer[UserLogData] = new FlinkKafkaConsumer[UserLogData](fromTopic, schema, consumerProperties)
      kafkaConsumer.setStartFromLatest()
//形成实时数据流
val dStream :DataStream[UserLogData] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
```



##### 读取消息队列数据

如上面示例代码所示，经过了大致3个步骤读取到kafka消息队列的数据
(1) 创建Flink实时上下文环境对象(上列已有不再重复)



(2) 连接kafka服务集群

```
Flink读取kafka采用的是connector方式
1 加载相关jar包(maven)
2 配置kafka集群相关参数及消费方式
3 kafka数据反序列化处理
4 调用相关API进行操作
```



配置kafka集群相关参数及消费方式

```scala
//集群参数(通过配置属性文件读取方式 consumerProperties)
val kafkaConsumer : FlinkKafkaConsumer[UserLogData] = new FlinkKafkaConsumer[UserLogData](fromTopic, schema, consumerProperties)

//消费方式：最早消息、最近消息、偏移量定位
kafkaConsumer.setStartFromLatest()
```



kafka数据反序列化处理

```scala
/**
* kafka数据反序列化处理
* 这样可以直接将消息中的json格式数据转换为对应case class对象
*/
class UserLogsKSchema(topic:String) extends KafkaSerializationSchema[UserLogData] with KafkaDeserializationSchema[UserLogData] {

  val gson : Gson = new Gson()

  /**
    * 反序列化
    * @param message
    * @return
    */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogData = {
    val key = record.key()
    val value = record.value()
    val log :UserLogData = gson.fromJson(new String(value), classOf[UserLogData])
    log
  }

  /**
    * 序列化
    * @param element
    * @return
    */
  override def serialize(element: UserLogData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val key = element.sid
    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }

  override def isEndOfStream(nextElement: UserLogData): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[UserLogData] = {
    return TypeInformation.of(classOf[UserLogData])
  }
```



用户行为日志数据对象

```scala
/**
    * 用户行为日志原始数据
    */
case class UserLogData(sid:String, userDevice:String, userDeviceType:String, os:String,
                       userID:String,userRegion:String, userRegionIP:String,                                    lonitude:String, latitude:String,manufacturer:String,                                    carrier:String, networkType:String, duration:String, exts:String,
                       action:String, eventType:String, ct:Long)
```



(3) 构建实时数据流

```
Flink对数据输入与输出分别称为：source 和 sink
1 通过设置输入数据source(FlinkKafkaConsumer对象)
2 设置并行度slot数量
```





### 第三节 实时数据ETL

#### 背景说明

```
实时场景下，原始数据可能存在不规范、较脏的情况，尤其是在高并发或需要处理的业务逻辑较复杂的情况下，对于实时场景的数据计算处理性能产生一定影响，这样需要我们对各类实时指标进行多阶段分步处理，比如第一步为数据规范处理(ETL),第二步为业务指标计算。经过第一步的数据清理工作后再发送到数据通道(kafka)中做消费计算，这样提高了实时处理性能同时数据复用得到了保障，同时对于有实时明细数据查询或动态聚合需求的情况可以结合其他技术进行实现(如druid可以对实时数据进行摄取提供对使用者的各种计算方式的支持)
最后强调一点：多步骤处理实时场景问题时是根据数据并发程度、计算复杂性、平台计算时效性等综合指标来确定的，在实际工作中要根据实际情况来进行处理，不必照搬这种设计方案！！！
```



#### 用户行为日志

##### 用户点击行为示例数据

```json
{
    "os": "2",
    "lonitude": "101.27417",
    "userRegion": "532823",
    "latitude": "21.45517",
    "eventType": "02",
    "userID": "32444",
    "sid": "20200103153500rgnuk",
    "manufacturer": "01",
    "duration": "0",
    "ct": "1578036900000",
    "carrier": "1",
    "userRegionIP": "67.77.242.139",
    "userDeviceType": "9",
    "KAFKA_ID": "74fk16e7bh",
    "action": "05",
    "userDevice": "94168",
    "networkType": "0",
    "exts": "{
		"eventTargetType":"101",
		"targetID":"P46"
	}"
}
```



正如上例所示，我们在进行点击日志统计时需要提取扩展字段(exts)中对应的数据信息，这些可能要作为统计维度参与业务统计，所以需要先进行实时数据ETL工作

```scala
val dStream :DataStream[UserLogData] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)

//交互点击行为
val clickDStream :DataStream[UserLogClickData] = dStream.filter(
        (log : UserLogData) => {
          log.action.equalsIgnoreCase(ActionEnum.INTERACTIVE.getCode) && log.eventType.equalsIgnoreCase(EventEnum.CLICK.getCode)
        }
      ).map(new UserLogClickDataMapFun())
```

(1) 数据过滤

```
使用Flink的filter算子
```

(2) 数据ETL

```scala
/**
    * 用户日志点击数据实时数据转换
    */
  class UserLogClickDataMapFun extends MapFunction[UserLogData,UserLogClickData]{

    override def map(value: UserLogData): UserLogClickData = {
      val sid :String = value.sid
      val userDevice:String = value.userDevice
      val userDeviceType:String = value.userDeviceType
      val os:String = value.os
      val userID :String = value.userID
      val userRegion :String = value.userRegion
      val userRegionIP:String = value.userRegionIP
      val lonitude:String = value.lonitude
      val latitude:String = value.latitude
      val manufacturer:String = value.manufacturer
      val carrier:String = value.carrier
      val networkType:String = value.networkType
      val action:String = value.action
      val eventType:String = value.eventType
      val ct:Long = value.ct
      val exts :String = value.exts
      var targetID :String = ""
      var eventTargetType :String = ""
      if(StringUtils.isNotEmpty(exts)){
        val extMap :mutable.Map[String,AnyRef] = JsonUtil.gObject2Map(exts)
        targetID = extMap.getOrElse(QRealTimeConstant.KEY_TARGET_ID, "").toString
        eventTargetType = extMap.getOrElse(QRealTimeConstant.KEY_EVENT_TARGET_TYPE, "").toString
      }

      UserLogClickData(sid, userDevice, userDeviceType, os,
        userID,userRegion, userRegionIP, lonitude, latitude,
        manufacturer, carrier, networkType,
        action, eventType, ct, targetID, eventTargetType)

    }
  }
```



(3) 得到清洗过后的数据对象

```scala
/**
    *用户行为日志点击操作数据
    */
case class UserLogClickData(sid:String, userDevice:String, userDeviceType:String, os:String,userID:String,userRegion:String, userRegionIP:String, lonitude:String, latitude:String,manufacturer:String, carrier:String, networkType:String,action:String, eventType:String, ct:Long, targetID:String, eventTargetType:String)

```



##### 用户产品列表浏览示例数据

```json
{
    "os": "1",
    "lonitude": "105.72017",
    "userRegion": "520381",
    "latitude": "28.45833",
    "eventType": "01",
    "userID": "59229",
    "sid": "20200103153500rkfxn",
    "manufacturer": "09",
    "duration": "54",
    "ct": "1578036900000",
    "carrier": "1",
    "userRegionIP": "64.234.5.53",
    "userDeviceType": "2",
    "KAFKA_ID": "ne31j8m79k",
    "action": "05",
    "userDevice": "25941",
    "hotTarget": "530829", 热门目的地
    "networkType": "3",
    "exts": "{
			"travelSendTime":"202002",
			"travelTime":"9",
			"productLevel":"4",
			"targetIDS":"["P40","P46","P28","P62"]",
			"travelSend":"520381",
			"productType":"01"
	}"
}
```



###### 背景说明

```

```



### 第四节 实时宽表数据构成



### 第五节 实时数据窗口统计



### 第六节 实时数据非开窗统计





===之前======

### 实时场景【非】

### 窗口应用场景【通用】

#### 窗口划分

##### 基于时间

```
基于时间的窗口统计（短周期窗口）站在自然时间属性角度下的数据分割(无论是事件时间还是处理时间)
		滚动窗口
			10分钟的PV、UV等等
		滑动窗口
			近xx秒|分的统计结果
```



##### 基于数量

```
基于数量的窗口统计（不固定长短）： 站在某种行业常规数据范围内的角度下的数据分割
		拉取N个事件数据后分析此批内的用户特征
			重点分析的行为日志
			业务相关的活动(打折、促销)
```



##### 基于会话【行业特点】

行业分析（算法介入）

```
短视频用户使用时间相对较长，小时+

资讯类用户使用时间 小时+|半小时+ 同时也跟用户习惯有关

特定领域：
音乐：
	跟时间有关 个人时间（小时+|半小时+） 工作时间（分钟+）
电商、旅游：
	依赖于用户习惯
车票：
	分钟+
	
```



```
基于会话的窗口统计（不固定长短，较基于时间窗口偏长）： 站在业务特点的角度下的数据分割
	* 窗口如何划分(及数据如何归属窗口)： 以符合行业特点设置的会话时间间隔gap作为划分依据
	* 窗口如何触发(trrigger)、长时间窗口下的计算结果如何近实时获取
		热门商品：
			分析当天N次会话中的用户行为日志统计（点击流）、业务相关的购买流等等	
```



#### 窗口统计

##### 热点倾斜

```
聚合函数(KeyBy等)产生热点

1 散裂化分组key
2 预聚合(window.reduce 类似map端reduce)
```





#### 窗口排序

【应用场景】：热门作品TopN，基于此的视频分类TopN

```
1 窗口内排序
2 全局排序
```



#### 窗口去重

【应用场景】：实时数仓中的ETL过程

```
1 基于HyperLogLog算法
依赖： hll库包

2 基于flink|spark sql
```





### 宽表数据处理【通用】

#### 静态维度数据

```
首次加载+广播方式 
		维表数据关联 -> 广播方式 -> 形成宽表
```



#### 动态维度数据

```
动态加载(异步方式) 
		维表数据关联 -> 异步IO（AsyncDataStream）-> RichAsyncFunction(因为是外部数据)
		超时timeout+缓存处理
```



### 规则引擎（drools）

```
实时数据结合规则进行数据选取或过滤
	基于停留时长的数据过滤
	基于业务需求的规则处理
```





### 实时数仓【通用】

```
实时数仓需求
	* 实时统计
		开窗统计
		全局统计
	* 实时明细
		Druid | ES -> 交互式查询
	* 实时多维计算
		Kylin
	* 全局累计计算
		结合update存储方案：hbase|es|redis
	* 实时数据ETL+复用
		多流程的实时计算，结合数据打回数据通道(kafka)

```



### 实时综合优化【通用】

```
（1） 反压机制 

（2） 算子链(Operator Chains): 减少资源开销(cpu)，序列化反序列化次数

（3） 实时计算并行度
	3.1 操作算子的计算并行度
		例如：datastream.setParallelelism(x)
	3.2 执行环境的计算并行度
		例如： env.setParallelism(x)
	3.3 客户端层面
		例如： flink提交 -p x 指定
	3.4 系统层面
		例如：flink的配置参数 flink-conf.yaml/parallelism.default
		
（4） 容错&状态存储
	4.1 容错
		checkpoint 同时处理快照
		
	4.2 状态存储
	内存级别 MemoryStateBackend
	缓存级别 RocksDBStateBackend
	
（5） 数据分区策略
	flink中的8种数据分区策略
        分组分区KeyGroupStreamPartitioner
            特点： 业务驱动的数据划分，hash方式
        广播分区BroadcastPartitioner
            特点：大小数据join，下发下游所有算子实例
        上下游分区结合RescalePartitioner
            特点：数据处理加速（类比spark的repartition）
        上下游相同分区ForwardPartitioner
            特点：上下游并行度一致
        随机分配分区ShufflePartitioner	
            特点：数据随机发送下游task
        全局分区GlobalPartitioner
            特点：上游数据发送下游单一分区进行全局相关计算（考虑数据量级和性能）
        自定义分区
            特点：划分方式自定义，灵活
```



### 实时综合应用场景

终极目标：实时分析平台

```
(1) 技术选型
	实时计算：flink
	实时数据存储：druid（实时场景数据复用）
	实时 明细+微聚合 数据查询&统计：druid
	实时明细数据查询：es
	实时累加：es（局部更新）+ grafana（BI可视化）
	实时多维计算：kylin
	实时数仓的中间数据（dwd、dws）：druid做二次加工
		过程：原始数据->kafka->flink(首次ETL)—>kafka->flink（窗口计算）+ flink+es(累计) -> 基于druid的动态多维度统计(不同于kylin和离线多维计算，维度数量和粒度相对较粗、较少)
        
（2） 应用场景
【1】近X分钟|秒内的统计指标
	特点：以时间维度切分连续的时间范围并对其各个范围分别进行计算任务
	滑动窗口

【2】数量限制场景或行业经验值限制场景
	特点：不指定窗口的结束时间，同时要求只能以接收此类数据并达到一定量级后才可触发窗口结束，进而统计其窗口内的计算任务。
	特价活动（数量限制）等
	
【3】用户画像&实时推荐
	特点：不知道窗口的结束时间：仅仅只能认为没有实时数据时是会话的结束
	用户实时偏好：
		* 会话窗口 sessionwindow
		* 长时间窗口中间数据输出 trigger触发
		* 会话级的基于用户个人的统计结果

总结：
窗口相关的需求，可以按照窗口长短、如何触发进行组合，一般来说滚动窗口、滑动窗口属于短窗口不需要显示指定触发器，而计数窗口和会话窗口一般属于长窗口，在实际项目中往往会有【长窗口定时输出中间结果】的需求，这样就需要结合触发器来达到目的。
		
		
【4】宽表构建
	* 维表数据使用【小】
	* 维表数据近实时使用(异步读取)【小】
	* 维表数据【大】处理
	* 维表数据读取有效性：
		超时问题（读取维表数据）
		加入缓存方案
	 
【5】实时数据去重：HLL库
	* 大量的UV统计指标
	
【6】实时TopN
	* 往往结合热门xx和推荐进行操作
	* 各种分类的排行榜（基于实时统计聚合结果）
	
【7】规则引擎触发数据选取&匹配
	* 规则触发业务数据选取
	* 规则触发维度数据选取或加工
	* 规则触发数据过滤(风控、反爬、用户筛选(基于用户画像得分等)等)
	
【8】基于实时数仓的OLAP实时分析

【9】基于实时数仓，为实时推荐等算法进行数据准备
```











## 第四章 技术方案【通用】

### 富客户端

```
1 对于某些业务需求来说
  会将部分计算任务来说会将其逻辑在app终端处理，同时app终端定与服务后台进行交互下载必要的数据进行本地计算处理，这样减少服务端压力，同时在【资源竞争类】场景下可以做好数据处理，如抢购XX功能下的数据校验（当然是指实体产品）
```



### 消息通道方式进行数据采集和流量削峰

```
1 传统数据采集使用负载均衡和同步处理
2 数据消息形式产生并异步处理(实时性、缓解高并发)
```



### 冷热数据隔离

```
1 如何形成热数据
2 冷数据如何处理
```





资料参考：

1 旅游行业数据分析 <http://forex.cngold.com.cn/20191124d1709n340854423.html>

