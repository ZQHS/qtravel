基于flink实时旅游平台

概要设计说明书

作        者：千锋大数据团队

完成日期：20200118

签  收  人：千锋大数据团队

签收日期：20190120

 

修改情况记录：

| 版本号 | 修改批准人 | 修改人 | 安装日期 | 签收人 |
| ------ | ---------- | ------ | -------- | ------ |
| 1.0    |            |        | 20190115 |        |
| 1.1    |            |        | 20200215 |        |
|        |            |        |          |        |



## 第一章 课程计划

-  介绍项目背景 

-  项目总体概述 

- 项目功能描述 

- 项目架构 
- 实时计算平台 
- 实时BI可视化



## 第二章 项目背景

### 第一节 背景描述

​		“旅游大数据”是指在旅游的“住行游购娱”六要素领域所产生的数量巨大、快速传播、类型多样相关（有结构和非结构的）、富有价值的数据集合，并且可以通过大数据技术（例如云计算、分布式存储、流运算、大数据算法、NoSQL数据库、SOA结构体系等）进行数据相关性分析和数据可视化，从而使游客消费者的决策更加有效便捷，提高满意度。

​		随着大数据技术在各行各业的落地为企业和用户提供了真实有意义的帮助，但传统的离线计算也随着人们需要了解数据实时情况的迫切需求而导致问题日益突出，实时流式大数据分析正在成为人们新的关注点，旅游行业也不例外，企业也需了解用户的实时行为、尤其是异常动作，另外实时推荐也是越来越起到增加用户粘性、加强用户体验、改进运营方向的重点任务。

​		在传统大数据架构的基础上，流式架构数据全程以流的形式处理，在数据接入端将ETL替换为数据通道。经过流处理加工后的数据，以消息的形式直接推送给了消费者。存储部分在外围系统以窗口的形式进行存储。适用于预警、监控、对数据有有效期要求的情况。





### 第二节 行业特征



![1582083367595](pic\travel_users.png)



- 用户数量：近亿级[2019年数据会有变化，但前几位基本在千万级别，另外携程与去哪儿合并]

```
【携程】  亿级 
【去哪儿】 亿级
【飞猪】   千万级
【马蜂窝】 千万级 
【途牛】   千万级
```

​	活跃用户：千万级

```
【携程】  千万级(5588万) 
【去哪儿】 千万级(4133万)
【飞猪】   千万级(2888万)
【马蜂窝】 千万级(2294万)
【途牛】   近千万级(944万)
```



- 数据量级

```
假设单条数据大小大约为1KB 
1 业务数据总量： 业务数量 * 活跃用户 * 用户系数 * 业务行为数量

2 数据量估算
示例 
用户行为数量= 4096W（活跃用户） * 1.5 (用户系数) * 1KB(单条数据大小) * 20(数据条) 【20 - 100范围】
          = 10000 * 1.5 * 4MK * 20 【20 - 100范围】
          = 1200GK
订单业务 = 4096W（活跃用户）* 0.01（订单百分比）* 1KB(单条数据大小)
        = 10000 * 0.01 * 1MK
        = 100GK
酒店和车票业务相对于旅游订单属于高频业务（发生次数较多），尤其是日志数据

3 总结：综合以上场景基本上日均数据量大约为TB级别
  注意：
	(1) 由于企业知名度和用户认可度不同，再加上旅游行业与季节、时间关系密切，可能在局部时间范围内会出现高并发的海量数据，个别时间呈现极地的数据量，所以数据量只能是个估算值，只要接近企业的大概数据级别即可。
    (2) 可使用网络公开的数据估算，如XX报告：每天TB级的增量数据，近百亿条的用户数据，上百万的产品数据     
```

​	 



- 集群规模(实时相关)

```
Job数: 
	30W+
Hadoop集群：
	2000+
消息规模：
	Topic 1300+、增量 100T+ PD、Avg 200K TPS、Max 900K TPS

注意：集群按不同技术分别有存储、消息通道、搜索等等，另外不同的业务重要程度、数据量级也不同，即使是使用同一种技术在优化技术方案时也会拆分不同集群，再加上容器化、虚拟化技术的运用，数据也在不断增加，所以集群规模不是一成不变的。

```



- 用户使用习惯

```
	用户旅游订单不会高频率呈现，但会集中于个别时间节点(如公共假期、周末、寒暑假等)高并发的呈现，另外作为一种“特殊”的消费方式，它还和社交媒体、社区等进行信息交互共享提高旅游品牌、旅游地的口碑、宣传推广等相关效应。
```



- 平台业务介绍

  - 核心业务&用户行为日志&访问日志

    - 基于旅游产品业务、酒店住宿业务、车票业务、其他衍生业务产生的数据进行相关指标计算后，获取各类运营报表。
    - 基于用户行为日志数据为用户画像、用户群体画像、实时推荐提供底层数据支持
    - 基于访问日志进行反爬虫分析

    - 基于平台产生的各类实时数据制作实时报表、构建实时数仓、构建实时关联关系(考虑效率可能离线处理)

      

  - 用户画像&用户群体画像

    - 用户画像

      - 静态标签：用户的性别、地域、职业、生活习惯等

      -  动态标签：用户关注的产品类别、产品偏好、内容偏好

    - 用户群体画像

      -   基于用户使用环境、自然特点、行为特征、喜爱偏好等方面形成的群体划分

      ​         用户使用环境(地域、渠道、使用网络、使用设备、版本等)

      ​         用户特点(性别、年龄、基本兴趣爱好等自然属性)

      ​		 用户行为特征(使用时间、使用时长、使用频率、各类行为统计)

      ​         用户喜好(跟行业相关，简单说就是各类产品中的关注甚至是交易情况)

    - 备注：

      实时推荐需要使用离线标签和实时标签，分别赋予对应权重再使用相关算法才能得出相对可靠准确的推荐结果。

       

  - 数据通道

    - 各类数据的流向通道

    - 解决系统间解耦、高并发问题、数据时效性等问题

    - 系统间的数据共享

      

  - 平台运维

    -  针对各类服务接口、大数据相关集群运行、作业处理等的监控、报警

    



## 第三章 项目概述

### 第一节 系统功能

| 功能模块     | 备注                                                         |
| ------------ | ------------------------------------------------------------ |
| 核心业务     | 基于核心业务数据(包括旅游订单、酒店住宿、车票业务)的相关实时计算指标、实时展示 |
| 用户行为日志 | 基于产品设计的各种用户行为埋点数据的相关实时计算指标、实时展示 |
| 风控报警     | 基于用户异常行为数据进行监控并实时报警展示                   |



​		本项目以大数据实时平台为终极目标，有关实时场景涉及的技术环节以下表呈现，平台按照涉及场景的技术方向构成，成为通用性的技术解决方案来解决实际业务需求。

| 技术方向     | 备注                                                       | 版本 |
| ------------ | ---------------------------------------------------------- | ---- |
| 实时数据ETL  | 实时数据清洗、去噪最终形成规范化数据                       | V1.0 |
| 实时数据统计 | 实现各种实时统计指标                                       | V1.0 |
| 实时数据存储 | 实时数据落地持久化为交互式搜索、动态查询计算提供技术支持   | V1.0 |
| 规则处理     | 主要服务于实时风控、报警等相关需求                         | V1.0 |
| 交互式查询   | 交互式查询明细数据或实时聚合数据(Clickhouse、Apache Druid) | V2.0 |
| 实时数据展示 | 主要服务于数据使用方，提供直观的展示形式                   | V2.0 |



### 第二节 数据流程描述

​		在大数据实时场景下，原始数据源基本上都是通过【消息通道】来完成数据采集、计算的过程。随后根据实际，提供了不同的分布式数据存储方案来满足数据使用方的交互式查询需求(在实时场景下，【时序库】存储形式是一种较好的选择)。最终，统计数据或风控数据应该以BI可视化的形式呈现给数据使用方。



![](pic\realtime_data.png)



### 第三节 数据组成

#### 事实数据

##### 旅游订单数据

```
用户ID：(在一些场景下，平台会为用户构造的唯一编号)
	userID
用户手机号：
	user_mobile
旅游产品编号：
	product_id: "598459284410"
旅游产品交通资源：
	product_traffic: 旅游交通选择
旅游产品交通资源：座席
	product_traffic_grade： 商务|一等|软卧...
旅游产品交通资源：行程种类
	product_traffic_type：单程|往返
旅游产品住宿资源：
	product_pub: 旅游住宿选择
所在区域：
	user_region
人员构成_成人人数：
	travel_member_adult
人员构成_儿童人数：
	travel_member_yonger
人员构成_婴儿人数：
	travel_member_baby
产品价格：
	product_price
活动特价	
	has_activity：0无活动特价|其他为折扣率如0.8
产品费用：
	product_fee
旅游订单：
	order_id	
下单时间：
	order_ct
```



![](pic\xc_order.png)



###### 旅游订单数据示例

```json
{
    "userID": "24225",
    "user_mobile": "18576342312",
    "product_id": "210609887",
    "product_traffic": "01",
    "product_traffic_grade": "11",
    "product_traffic_type": "01",
    "product_pub": "210609887|dd40d0e8",
    "userRegion": "330500",
    "travel_member_adult": "2",
    "travel_member_yonger": "1",
    "travel_member_baby": "0",
    "product_price": "5",
    "has_activity": "9",
    "product_fee": "13",
    "order_id": "158201336000021060988724225"
    "order_ct": "1582013360000",
    "KAFKA_ID": "a55d114612885e3ff5d93f4fb271e972"  
}
```



##### 用户行为日志



用户搜索行为

![](pic\xc_search_condition.png)



用户浏览产品列表

![1582103115865](pic\xc_search_list.png)



```
行为类型：
 	action： 'launch启动| interactive交互| page_enter页面曝光(产品页展示)',
事件类型：
  	eventType： 'view浏览（多产品）| slide滑动 (多产品)|click点击(收藏|点赞|分享)',
用户ID：(在一些场景下，平台会为用户构造的唯一编号)
	userID
所属设备号(app端的手机设备号)：
	userDevice
操作系统：
	os
手机制造商：
	manufacturer
电信运营商：
	carrier
网络类型：
	networkType
所在区域：
	userRegion
所在区域IP:
	userRegionIP
经度:
	longitude
纬度:
	latitude
扩展信息
	exts
事件发生时间：
	ct
```



###### 用户行为日志示例

具体参考（qdata项目：com.qf.bigdata.realtime.enumes.ActionEnum、EventEnum、EventActionEnum）

```java
#行为种类（ActionEnum）：
INSTALL("01", "install","安装"),
LAUNCH("02", "launch","加载启动"),
LOGIN("03", "login","登录"),
REGISTER("04", "register","注册"),
INTERACTIVE("05", "interactive","交互行为"),
EXIT("06", "exit","退出"),
PAGE_ENTER_H5("07", "page_enter_h5","页面进入"),
PAGE_ENTER_NATIVE("08", "page_enter_native","页面进入")

#动作事件类型（EventEnum）：
VIEW("01", "view","浏览"),
CLICK("02", "click","点击"),
INPUT("03", "input","输入"),
SLIDE("04", "slide","滑动")
    
#事件的行为目的（EventActionEnum）：
PRODUCT_KEEP("101", "收藏"),
PRODUCT_APPLAUD("102", "点赞"),
PRODUCT_SHARE("103", "分享"),
PRODUCT_COMMENT("104", "点评"),
PRODUCT_CS("105", "客服")
```



行为事件种类说明：

(1) 启动日志

```
action=02 #注释：(launch启动)
eventType=无交互事件
exts=无扩展信息
```

示例

```json
{
	"os":"1",
	"lonitude":"115.27267",
	"userRegion":"130533",
	"latitude":"36.90133",
	"eventType":"",
	"userID":"85662",
	"sid":"20200103153500jdjqx",
	"manufacturer":"09",
	"duration":"38",
	"ct":"1578036900000",
	"carrier":"3",
	"userRegionIP":"27.32.4.174",
	"userDeviceType":"9",
	"KAFKA_ID":"a4hm6akmmj",
	"action":"02",
	"userDevice":"51822",
	"networkType":"1",
	"exts":""
}
```



(2) 页面浏览日志

```
action=07 | 08 #注释：page_enter_native 08 | page_enter_h5 07 产品页面进入
eventType= 01 #注释： view 浏览
exts={
	targetID: [目标页面]
}
```

示例

```json
{
	"os":"1",
	"lonitude":"115.27267",
	"userRegion":"130533",
	"latitude":"36.90133",
	"eventType":"01",
	"userID":"85662",
	"sid":"20200103153500jdjqx",
	"manufacturer":"09",
	"duration":"38",
	"ct":"1578036900000",
	"carrier":"3",
	"userRegionIP":"27.32.4.174",
	"userDeviceType":"9",
	"KAFKA_ID":"a4hm6akmmj",
	"action":"08",
	"userDevice":"51822",
	"networkType":"1",
	"exts":"{"targetID":"P1"}"
}
```



(3) 交互式日志

(3-1) 点击日志

```
action=05 #注释：interactive 交互式
eventType=02 #注释：click 点击
exts={
	targetID: [目标页面]
	eventTargetType: [目标动作类型（关注、点评、分享、收藏）]
}
```

示例

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



(3-2) 产品列表浏览日志

```
action=05 #注释：interactive 交互式
event_type=01 | 04 #注释： view浏览|slide滑动
extinfo={
	targetIDS: [目标页面列表]
	productType: [产品类型：跟团、私家、半自助等]
	productLevel: [产品钻级 1-5]
	travelTime: [行程天数]
    travelSendTime: [出发时间]
    travelSend: [出发地]      		
}
```

示例

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



#### 维度数据

##### 旅游产品维表

```mysql
CREATE TABLE travel.dim_product (
  product_id text NULL COMMENT '旅游产品订单ID',
  product_level int(11) DEFAULT 0 COMMENT '旅游产品订单级别',
  product_type text NULL COMMENT '旅游产品订单类型',
  departure_code text NULL COMMENT '旅游出发地编码',
  des_city_code text NULL COMMENT '旅游目的地编码',
  toursim_tickets_type text NULL COMMENT '旅游产品订单类型(编码)'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```



![1582019139517](pic\dim_product.png)



##### 酒店维表

```mysql
CREATE TABLE travel.dim_pub (
   pub_id text NULL COMMENT '酒店ID',
   pub_name text NOT NULL COMMENT '酒店名称',
   pub_stat int(11) DEFAULT 0 NOT NULL COMMENT '酒店星级',
   pub_grade text NULL COMMENT '酒店等级编码',
   pub_grade_desc text NULL COMMENT '酒店等级描述',
   pub_area_code text NULL COMMENT '酒店所在地区',
   pub_address text NULL COMMENT '酒店地址',
   is_national text NULL COMMENT '境内外编码'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```



![1582019294968](pic\dim_pub.png)



## 第四章 逻辑架构设计

- 逻辑架构设计图



![](pic\travel_logic_architecture.png)



## 第五章 功能描述

- 实时数据ETL（通用）

  实时场景下的数据规范，适合大部分业务数据、日志数据，相当于实时数仓中的dw。

  示例：用户浏览产品列表日志数据需要进行数据ETL形成符合要求的规范数据(将列表拆分后形成明细数据)



- 实时数据宽表构建（通用）

  实时场景下的事实数据关联维表数据后形成宽表数据为下游进行分组聚合提供技术支持(这里不同于离线数仓，由于时效性要求，为了提高计算结果，一般不会持久化类似dm数据进行再次加工使用)。

  示例：旅游订单数据包括了产品维表外键，需要关联后使用维表数据中的维度进行分组聚合计算。



- 实时数据查询

  实时明细数据：实时场景下的任何明细数据需要持久化到时序库中才能满足交互式查询需求。

  示例：实时订单数据持久化到ES、ApacheDruid、Clickhouse，客户端通过读取这些存储介质获取数据

  

  实时聚合数据：实时场景下的窗口聚合或累计聚合也需要持久化持久化介质才能满足交互式查询需求。

  示例：10分钟滚动窗口内的订单统计、消费统计、PUV统计等



- connector应用

  常用的connector：jdbc、kafka、es、redis

  实时数据输出ES，虽然Flink有ES的connector，但无法使用es的局部更新，所以需要用户自定义ES Sink，另外也熟悉使用自定义sink。

  示例：旅游订单输出kafka、es，其中维表数据使用flink-jdbc方式读取。



- 窗口应用

  (1) 基于事件时间的滚动窗口(TumblingEventTimeWindows)统计。

  ​     示例：固定N分钟统计结果（实际事件发生时间）
  (2) 基于事件时间的滑动窗口(SlidingEventTimeWindows)统计。

  ​     示例：近N分钟统计结果（实际事件发生时间）
  (3) 基于处理时间的滚动窗口(TumblingProcessingTimeWindows)统计。

  ​	 示例：固定N分钟统计结果（任务处理节点机器时间）
  (4) 基于处理时间的滑动窗口(SlidingProcessingTimeWindows)统计。

  ​     示例：近N分钟统计结果（任务处理节点机器时间）

  (5) 基于全局窗口(timeWindowAll)统计

  ​	示例：全局窗口统计基于所有数据的统计结果。	

  

- 自定义触发器

  窗口周期较长时，如果想依据数据量或间隔时间输出窗口的中间计算结果，需要使用内置触发器或自定义触发器。

  示例：基于数据量或间隔时间的自定义触发器



- 状态维护

  实时计算时需要维护中间过程数据即状态维护

  示例：窗口周期内的UV、PV、极值等计算



- 非窗口方式统计计算

  在不开窗的情况下，如何进行数据聚合计算，即自定义实现process处理函数

  示例：订单非窗口累计聚合计算



- CEP应用

  复杂事件处理应用场景示例，一般在风控领域或异常数据监测应用较多，本质是定义规则

  示例：用户浏览页面的停留时长应该符合行业特点的[m,n]时间之内，不符合的需要进行监控报警处理。



- 数据实时采集

  简单来说即通过消费kafka数据进行数据落地，解决了数据使用的时效性问题

  示例：旅游订单数据实时采集落地





## 第六章 系统架构



### 技术架构图

![](D:/qfBigWorkSpace/qtravel/doc/%E9%A1%B9%E7%9B%AE%E6%96%87%E6%A1%A3/pic/travel_bigdata_platform.png)





### 实时场景技术方案



![](pic\realtime_dataflow.png)



### 技术框架组成

| 框架名称      | 备注           | 版本    |
| ------------- | -------------- | ------- |
| Flink         | 实时计算       | 1.9.1   |
| Kafka         | 消息通道       | 1.1+    |
| Elasticsearch | 全文搜索       | 6.x+    |
| Apache Druid  | 时序库         | 0.13+   |
| Hadoop        | 分布式文件系统 | 2.7.x + |



### 技术选型说明

```
一 数据来源及采集
	1  埋点数据： 
	   由前端(js或手机终端)发送后台微服务相关埋点数据
	2  业务相关数据：
       (1) 高并发复杂业务场景下原数据同步处理方式改进为异步消息处理方式
       (2) 变更日志数据(如mysql的binlog)
    3  外部接口数据：
       (1) 高并发复杂业务场景下高并发场景下原数据同步处理方式改进为异步消息处理方式
   上述是大部分平台都会使用的数据来源，接下来就是可能由采集框架(flume或logstash等)或微服务后台充当消息生产者进行消息生产(producer)    


二 消息通道
	(1) 基于kafka的消息通道用于消息生产端与消费端间的系统解耦
	(2) 基于kafka的消息通道用于改进高并发场景下的数据处理效率(同步方式->异步方式)


三 实时计算
	Flink提供了实时场景下数据ETL、各种统计方式(包括触发方式)、规则报警等技术支持，并提供批数据处理、图计算、机器学习等功能，成为一个综合技术栈。此外还可以结合其他框架(存储、搜索、多维分析、交互查询等)共同打造大数据平台。
	Kylin做为多维分析计算框架也会被应用于实时场景，但这跟行业及企业需求有关。
	

四 数据存储
	简单来说，不论离线还是实时场景一般数据可分为明细数据和统计数据。实时场景下数据处理完成后如何被数据使用方使用或者提供数据使用方哪些数据使用功能呢？
	明细数据： 数据使用方查询或搜索使用(尤其是重要的业务数据)，这样就需要像ES类似的搜索框架，当然如果是查询需求还可以使用Druid这类时序库来查询数据或做二次加工分析，或者Clickhouse
	统计数据： 统计数据一般级量较小(除非是高基维分组统计)一般是结合BI可视化进行实时展示的(像ES、Druid都提供明细或聚合数据的高效的读写功能)，另外部分场景可能被写入缓存中供其他系统调用(如Redis)
		
		
五 数据展示：
	实时展示： Grafana
		
```



## 第七章 项目开发

### 第一节 开发准备工作

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
	<artifactId>flink-jdbc_2.11</artifactId>
	<version>1.9.1</version>
</dependency>
```



### 第二节 实时计算上下文环境构建

#### 背景说明

```
对于使用Flink框架来处理实时场景来说，首先是要构建的是StreamExecutionEnvironment，当然Flink也提供了处理离线场景的对应上下文对象ExecutionEnvironment
基于功能复用的思想我们封装了一些常用的功能形成Flink帮助类：FlinkHelper，下面这段代码提取了构建上下文环境对象的函数。
注释：com.qf.bigdata.realtime.flink.util.help.FlinkHelper
```



#### 构建实时上下文环境对象

```scala
/**
    * 流式环境flink上下文构建
    * @param checkPointInterval 检查点时间间隔
    * @return
    */
  def createStreamingEnvironment(checkPointInterval :Long, tc :TimeCharacteristic, watermarkInterval:Long) :StreamExecutionEnvironment = {
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

      //时间语义
      env.setStreamTimeCharacteristic(tc)

      //创建水位时间间隔
      env.getConfig.setAutoWatermarkInterval(watermarkInterval)

    }catch{
      case ex:Exception => {
        println(s"FlinkHelper create flink context occur exception：msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }
    env
  }
```



### 第三节 常用资源连接

#### 背景说明

```
Flink作为计算框架必然会涉及其他资源型框架的读写工作，如存储框架Hadoop、HBase，消息通道Kafka等，当然kafka所能连接的框架还有很多，大部分第三方的连接源称为connector，按照输入或输出位置分为了source和sink，下面我们列举了在实时场景下常用的几种source或sink。
1 kafka消息通道
2 jdbc数据源
3 redis缓存
```



kafka通道操作

```shell
1 创建topic
${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper node242:2181,node243:2181,node244:2181/kafka --create  --replication-factor 2 --partitions 5 --topic topic_orders_ods


2 查看topic
${KAFKA_HOME}/bin/kafka-topics.sh  --zookeeper node242:2181,node243:2181,node244:2181/kafka --list


3 console 消费消息
${KAFKA_HOME}/bin/kafka-console-consumer.sh --bootstrap-server node242:9092,node243:9092,node244:9092 --topic topic_orders_ods

```



#### kafka集群配置参数

配置文件：【scr/resource/kafka/flink/kafka-consumer.properties】

```properties
zk.connect=node242:2181,node243:2181,node244:2181/kafka

#broker list
bootstrap.servers=node242:9092,node243:9092,node244:9092

#kafka message 序列化 IntegerDeserializer
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

#消费组
group.id=qf_flink_kafka

#request.timeout.ms should be greater than session.timeout.ms and fetch.max.wait.ms
request.timeout.ms=60000

#consumer-kafka(broker controller)( <= 1/3 * session.timeout.ms)
heartbeat.interval.ms=15000

#consumer group
#broker group.min.session.timeout.ms(6000) < ? < group.max.session.timeout.ms(30,0000)
session.timeout.ms=40000

#consumer
fetch.max.wait.ms=5000

##fetch min
fetch.min.bytes=0

#fetch.max(message.max.bytes=1000012 10m, producer max.request.size=1048576 10m)
fetch.max.bytes=1000012

# (broker message.max.bytes=1000012 10m )
max.partition.fetch.bytes=1000012

#offset [latest, earliest, none]
auto.offset.reset=latest

#kafka commit
enable.auto.commit=true
auto.commit.interval.ms= 2000

#consumer
#max.poll.interval.ms=20000

#consumer
#max.poll.records=500

#compression type=none, gzip, snappy, lz4, producer
#compression.type=snappy

#consumer producer receive.buffer.bytes=32768
#receive.buffer.bytes=65536

#consumer
#send.buffer.bytes=131072

#broker
#metadata.max.age.ms=300000

#consumer
#reconnect.backoff.ms=50
```



#### Kafka-Flink消费

- 基于字符串(kafka数据)的消费方式

```scala
/**
    * flink读取kafka数据
    * @param env flink上下文环境对象
    * @param topic kafka主题
    * @param properties kafka消费配置参数
    * @return
    */
  def createKafkaConsumer(env:StreamExecutionEnvironment, topic:String, groupID:String) :FlinkKafkaConsumer[String] = {
    //kafka数据序列化
    val schema = new SimpleStringSchema()

    //kafka消费参数
    val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    //重置消费组
    consumerProperties.setProperty("group.id", groupID)

    //创建kafka消费者
    val kafkaConsumer : FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic, schema, consumerProperties)

    //flink消费kafka数据策略：读取最新数据
    kafkaConsumer.setStartFromLatest()

    //kafka数据偏移量自动提交（默认为true，配合kafka消费参数 enable.auto.commit=true）
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    kafkaConsumer
  }
```



- 基于自定义实现序列化反序列化(kafka数据)的消费方式

```scala
/**
    * flink读取kafka数据
    * @param env flink上下文环境对象
    * @param topic kafka主题
    * @param schema kafka数据序列化|反序列化
    * @param properties kafka消费配置参数
    * @param sm kafka消费策略
    * @return
    */
  def createKafkaSerDeConsumer[T: TypeInformation](env:StreamExecutionEnvironment, topic:String, groupID:String, schema:KafkaDeserializationSchema[T], sm: StartupMode) :FlinkKafkaConsumer[T] = {
    //kafka消费参数
    val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    //重置消费组
    consumerProperties.setProperty("group.id", groupID)

    //创建kafka消费者
    val kafkaConsumer : FlinkKafkaConsumer[T] = new FlinkKafkaConsumer(topic, schema, consumerProperties)

    //flink消费kafka数据策略：读取最新数据
    if(StartupMode.LATEST.equals(sm)){
      kafkaConsumer.setStartFromLatest()
    }else if(StartupMode.EARLIEST.equals(sm)){//flink消费kafka数据策略：读取最早数据
      kafkaConsumer.setStartFromEarliest()
    }

    //kafka数据偏移量自动提交（默认为true，配合kafka消费参数 enable.auto.commit=true）
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)

    kafkaConsumer
  }
```





#### mysql配置参数

配置文件：【scr/resource/jdbc.properties】

```properties
jdbc.driver=com.mysql.jdbc.Driver
jdbc.user=root
jdbc.password=12345678
jdbc.url=jdbc:mysql://10.0.88.242:3306/travel?serverTimezone=UTC&characterEncoding=utf-8
```



#### JDBC-Flink读取

```scala
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
      .finish();

    jdbcInputFormat
  }


  /**
    * 维度数据加载
    * @param env flink上下文环境对象
    * @param sql 查询语句
    * @param fieldTypes 查询语句对应字段类型列表
    * @return
    */
  def createOffLineDataStream(env: StreamExecutionEnvironment, sql:String, fieldTypes: Seq[TypeInformation[_]]):DataStream[Row] = {
    //JDBC属性
    val mysqlDBProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.MYSQL_CONFIG_URL)

    //flink-jdbc包支持的jdbc输入格式
    val jdbcInputFormat : JDBCInputFormat= FlinkHelper.createJDBCInputFormat(mysqlDBProperties, sql, fieldTypes)

    //jdbc数据读取后形成数据流[其中Row为数据类型，类似jdbc]
    val jdbcDataStream :DataStream[Row] = env.createInput(jdbcInputFormat)
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
    val driver :String = properties.getProperty(TravelConstant.FLINK_JDBC_DRIVER_MYSQL_KEY)
    val url :String = properties.getProperty(TravelConstant.FLINK_JDBC_URL_KEY)
    val user:String = properties.getProperty(TravelConstant.FLINK_JDBC_USERNAME_KEY)
    val passwd:String = properties.getProperty(TravelConstant.FLINK_JDBC_PASSWD_KEY)

    //flink-jdbc包支持的jdbc输入格式
    val jdbcInputFormat : JDBCInputFormat = createJDBCInputFormat(driver, url, user, passwd,
      sql, fieldTypes)

    jdbcInputFormat
  }
```



#### redis配置参数

配置文件：【scr/resource/redis/redis.properties】

```properties
#redis服务器ip
redis_host=node11

#redis服务端口
redis_port=6379

#redis验证密码(如果有)
redis_password=qfqf

#redis操作超时
redis_timeout=10000

#redis所选库
redis_db=1

#redis最大等待连接中的数量
redis_maxidle=10

#redis最小等待连接中的数量
redis_minidle=2

#redis最大数据库连接数
redis_maxtotal=20
```



Redis-Flink写入数据

创建redis连接参数(FlinkHelper)

```scala
/**
 * redis连接参数(单点)
*/
def createRedisConfig() : FlinkJedisConfigBase = {

//redis配置文件
val redisProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.REDIS_CONF_PATH)

//redis连接参数
val redisDB :Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_DB).toInt
val redisMaxIdle :Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_MAXIDLE).toInt
val redisMinIdle:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_MINIDLE).toInt
val redisMaxTotal:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_MAXTOTAL).toInt
val redisHost:String = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_HOST)
val redisPassword:String = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_PASSWORD)
val redisPort:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_PORT).toInt
val redisTimeout:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_TIMEOUT).toInt

//redis配置对象构造
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
```



RedisMapper接口实现

```scala
/**
  * 自定义实现redis sink的RedisMapper接口
  * 订单聚合结果输出redis
  * @param redisCommand
  */
class QRedisSetMapper() extends RedisMapper[QKVBase]{

  /**
    * redis 操作命令
    * @return
    */
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET, null)
  }

  /**
    * redis key键
    * @param data
    * @return
    */
  override def getKeyFromData(data: QKVBase): String = {
    data.key
  }

  /**
    * redis value
    * @param data
    * @return
    */
  override def getValueFromData(data: QKVBase): String = {
      data.value
  }
}
```



写入redis数据

```scala
/**
 * 写入下游环节缓存redis(被其他系统调用)
 */
val ds : DataStream[T] = ... 
val redisMapper = new QRedisSetMapper()
val redisConf = FlinkHelper.createRedisConfig()
val redisSink = new RedisSink(redisConf, redisMapper)
ds.addSink(redisSink)
```



#### 项目配置常量

背景说明

```
项目开发过程中涉及到的配置参数、参数数值尽量用类常量设置，而不要使用硬编码方式，下面示例展示了部分主要的配置参数(摘自QRealTimeConstant)，全部参数请参考项目代码
注释：com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
```





### 第四节 数据通道

#### 背景说明

```
目前实时场景的数据来源一般是取自消息队列(如kafka)，下面我们看看如何使用Flink来进行kafka数据的读取工作。
我们以用户行为日志中的点击行为为示例进行说明，下例为用户点击行为明细处理类UserLogsClickHandler中的局部片段，具体完整代码请参考项目相关代码。
注释：com.qf.bigdata.realtime.flink.streaming.etl.ods.UserLogsClickHandler
```



#### 点击行为日志数据处理

##### 读取消息队列数据

(1) 创建Flink实时上下文环境对象

```scala
/**
 * 1 Flink环境初始化
 *   流式处理的时间特征依赖(使用事件时间)
 */
//注意：检查点时间间隔单位：毫秒
val checkpointInterval = QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL
val tc = TimeCharacteristic.EventTime
val watermarkInterval= QRealTimeConstant.FLINK_WATERMARK_INTERVAL
val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(checkpointInterval, tc, watermarkInterval)

```



(2) 连接kafka服务集群

```
Flink读取kafka采用的是connector方式
1 加载相关jar包(maven)，参考【开发准备工作】
2 配置kafka集群相关参数及消费方式
3 kafka数据反序列化处理
4 调用相关API进行操作
```



##### 配置kafka集群相关参数及消费方式

###### kafka集群参数

注释：请参考上面【kafka集群配置参数】



###### kafka消费方式

```scala
//kafka消费参数
val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    
//重置消费组
consumerProperties.setProperty("group.id", groupID)

//创建kafka消费者
val kafkaConsumer : FlinkKafkaConsumer[T] = new FlinkKafkaConsumer(topic, schema, consumerProperties)

//flink消费kafka数据策略：读取最新数据
if(StartupMode.LATEST.equals(sm)){
    kafkaConsumer.setStartFromLatest()
}else if(StartupMode.EARLIEST.equals(sm)){//flink消费kafka数据策略：读取最早数据
    kafkaConsumer.setStartFromEarliest()
}

//kafka数据偏移量自动提交（默认为true，配合kafka消费参数 enable.auto.commit=true）
kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
```



###### kafka数据反序列化处理

用户行为日志

```scala
/**
* kafka数据反序列化处理
* 这样可以直接将消息中的json格式数据转换为对应case class对象
*/
class UserLogsKSchema(topic:String) extends KafkaSerializationSchema[UserLogData] with KafkaDeserializationSchema[UserLogData] {


  /**
    * 反序列化
    * @param message
    * @return
    */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogData = {
    val key = record.key()
    val value = record.value()
    val gson : Gson = new Gson()  
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



###### 用户行为日志数据对象

```scala
/**
    * 用户行为日志原始数据
    */
case class UserLogData(sid:String, userDevice:String, userDeviceType:String, os:String,
                       userID:String,userRegion:String, userRegionIP:String,                                    lonitude:String, latitude:String,manufacturer:String,                                    carrier:String, networkType:String, duration:String, exts:String,
                       action:String, eventType:String, ct:Long)
```



#####  构建实时数据流

```
Flink对数据输入与输出分别称为：source 和 sink
1 通过设置输入数据source(FlinkKafkaConsumer对象)
2 设置并行度slot数量
```



```scala
//形成实时数据流
val dStream :DataStream[UserLogData] = env.addSource(kafkaConsumer).setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
```





### 第五节 实时数据ETL

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



###### 背景说明

```
正如上例所示，我们在进行点击日志统计时需要提取扩展字段(exts)中对应的数据信息，这些可能要作为统计维度参与业务统计，所以需要先进行实时数据ETL工作,按照之前的相关代码我们已经获取了kafka的数据并通过Flink读取形成数据流，如下列所示，具体代码参考UserLogsClickHandler
注释：com.qf.bigdata.realtime.flink.streaming.etl.ods.UserLogsClickHandler
```



###### kafka数据反序列化

```
用户点击行为日志的kafka数据反序列化参考 第四节【kafka数据反序列化处理】
```



###### 数据过滤

```scala
//交互点击行为
val clickDStream :DataStream[UserLogClickData] = dStream.filter(
        (log : UserLogData) => {
          log.action.equalsIgnoreCase(ActionEnum.INTERACTIVE.getCode) && log.eventType.equalsIgnoreCase(EventEnum.CLICK.getCode)
        }
      ).map(new UserLogClickDataMapFun())
```



###### 数据转换

注释：转换函数UserLogClickDataMapFun

```scala
/**
    * 用户日志点击数据实时数据转换（日志数据 -> 点击日志）
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
        val extMap :mutable.Map[String,AnyRef] = JsonUtil.gJson2Map(exts)  
          
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



###### 原始数据

```scala
/**
    * 用户行为日志原始数据
    */
  case class UserLogData(sid:String, userDevice:String, userDeviceType:String, os:String,
                          userID:String,userRegion:String, userRegionIP:String, lonitude:String, latitude:String,
                          manufacturer:String, carrier:String, networkType:String, duration:String, exts:String,
                          action:String, eventType:String, ct:Long)
```



###### ETL后的规范数据

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
请仔细观察【用户点击行为示例数据】和【用户产品列表浏览示例数据】，你会发现除了数据schema有些不同外最主要的是扩展信息exts中的【targetIDS】字段为json数组形式，结合业务含义，表达的是一个用户通过查询搜索获得的产品列表或用户在上下滑动手机进行类似翻页浏览产品页信息，但如果我们有【热门产品】统计、排名等需求时首先要将这些列表进行拆分，从软件功能上来讲称为"1拆多"(flatMap)，所以在后续做指标统计之前第一步先做好数据的ETL工作，具体如下列所示，具体代码参考UserLogsViewListHandler
注释：com.qf.bigdata.realtime.flink.streaming.etl.ods.UserLogsViewListHandler
```



###### kafka数据反序列化

```scala
 /**
  * 2 kafka流式数据源
  *   kafka消费配置参数
  *   kafka消费策略
  *   创建flink消费对象FlinkKafkaConsumer
  *   用户行为日志(kafka数据)反序列化处理
  */
val schema:KafkaDeserializationSchema[UserLogViewListData] = new UserLogsViewListKSchema(fromTopic)
      
val kafkaConsumer : FlinkKafkaConsumer[UserLogViewListData] = FlinkHelper.createKafkaSerDeConsumer(env, fromTopic, groupID, schema, StartupMode.LATEST)

```



注释：具体代码参考 com.qf.bigdata.realtime.flink.schema.UserLogsViewListKSchema

```scala
/**
  * 行为日志产品列表浏览数据(原始)kafka序列化
  */
class UserLogsViewListKSchema(topic:String) extends KafkaSerializationSchema[UserLogViewListData] with KafkaDeserializationSchema[UserLogViewListData] {

  /**
    * 反序列化
    * @param message
    * @return
    */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogViewListData = {
    val key = record.key()
    val value = record.value()
    val gson : Gson = new Gson()  
    val log :UserLogViewListData = gson.fromJson(new String(value), classOf[UserLogViewListData])
    log
  }

  /**
    * 序列化
    * @param element
    * @return
    */
  override def serialize(element: UserLogViewListData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val sid = element.sid
    val userDevice = element.userDevice
    val userID = element.userID
    val tmp = sid + userDevice+ userID
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)

    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }

  override def isEndOfStream(nextElement: UserLogViewListData): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[UserLogViewListData] = {
    return TypeInformation.of(classOf[UserLogViewListData])
  }


}
```



###### 列表数据处理

```scala
/**
 * 3 创建【产品列表浏览】日志数据流
 *   (1) 基于处理时间的实时计算：ProcessingTime
 *   (2) 数据过滤
 *   (3) 数据转换
 */
val viewListFactDStream :DataStream[UserLogViewListFactData] = env.addSource(kafkaConsumer)
   .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
   .filter(
          (log : UserLogViewListData) => {
            log.action.equalsIgnoreCase(ActionEnum.INTERACTIVE.getCode) && EventEnum.getViewListEvents.contains(log.eventType)
          }
        )
   .flatMap(new UserLogsViewListFlatMapFun())
viewListFactDStream.print("=====viewListFactDStream========")

```



###### 列表拆分处理

```scala
/**
    * 用户行为原始数据ETL
    * 数据扁平化处理：浏览多产品记录拉平
    */
  class UserLogsViewListFlatMapFun extends FlatMapFunction[UserLogViewListData,UserLogViewListFactData]{

    override def flatMap(value: UserLogViewListData, values: Collector[UserLogViewListFactData]): Unit = {

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
      val duration :String = value.duration
      val action:String = value.action
      val eventType:String = value.eventType
      val ct:Long = value.ct
      val hotTarget:String = value.hotTarget
      val exts :String = value.exts

      if(StringUtils.isNotEmpty(exts)){
        val extMap :mutable.Map[String,AnyRef] = JsonUtil.gJson2Map(exts)  
          
        val travelSendTime = extMap.getOrElse(QRealTimeConstant.KEY_TRAVEL_SENDTIME, "").toString
        val travelSend = extMap.getOrElse(QRealTimeConstant.KEY_TRAVEL_SEND, "").toString
        val travelTime = extMap.getOrElse(QRealTimeConstant.KEY_TRAVEL_TIME, "").toString
        val productLevel = extMap.getOrElse(QRealTimeConstant.KEY_PRODUCT_LEVEL, "").toString
        val productType = extMap.getOrElse(QRealTimeConstant.KEY_PRODUCT_TYPE, "").toString
        val targetIDSJson = extMap.getOrElse(QRealTimeConstant.KEY_TARGET_IDS, "").toString

        //列表拆分
        val targetIDS :util.List[String] = JsonUtil.gJson2List(targetIDSJson)
        for(targetID <- targetIDS){
          val data = UserLogViewListFactData(sid, userDevice, userDeviceType, os,
            userID,userRegion, userRegionIP, lonitude, latitude,
            manufacturer, carrier, networkType, duration,
            action, eventType, ct,
            targetID, hotTarget, travelSend, travelSendTime,
            travelTime, productLevel, productType)

          values.collect(data)
        }
      }
    }
  }
```



###### 原始数据

```scala
/**
    * 用户行为日志产品列表浏览数据
    */
  case class UserLogViewListData(sid:String, userDevice:String, userDeviceType:String, ·								os:String,userID:String,userRegion:String, 					 							userRegionIP:String, lonitude:String, latitude:String,
                                manufacturer:String, carrier:String, networkType:String, 								 duration:String, exts:String,action:String, 											eventType:String, ct:Long,hotTarget:String)
```



###### ETL后的规范数据

```scala
/**
    *用户行为日志产品列表浏览数据
    */
case class UserLogViewListFactData(sid:String, userDevice:String,userDeviceType:String, 					os:String,userID:String,userRegion:String, userRegionIP:String, 					  lonitude:String, latitude:String,manufacturer:String, carrier:String, 				  networkType:String, duration:String,action:String, eventType:String, 			          ct:Long,targetID:String,hotTarget:String,travelSend:String,
                  travelSendTime:String,travelTime:String, productLevel:String,                             productType:String)
```



### 第六节 实时宽表数据构成

#### 背景说明

```
如果我们做过离线数仓会知道有事实层、维表层、集市层等，集市层或中间层的统计数据、明细数据往往会是宽表数据或基于宽表数据聚合而来，在实时数仓中也是类似，根据需求我们会将消息通道的数据作为事实数据，把维表数据通过广播等方式加载到任务执行节点(taskManager)上进行宽表数据合成，进而后续可以完成不同的统计需求，但这里与多维计算模型不同(keylin处理多维计算模型的框架)，仅仅是在一定范围内的分组聚合，如你有多维计算的需求请直接使用kylin即可(kylin对离线和实时2种场景都支持)，所以下面我们聊聊宽表数据的构成过程，下面代码为主要代码，具体代码请参考项目, 下面示例是旅游产品订单业务。
注释：
1 广播方式 com.qf.bigdata.realtime.flink.streaming.etl.dw.orders.OrdersWideDetail2ESHandler
2 异步IO方式 com.qf.bigdata.realtime.flink.streaming.etl.dw.orders.OrdersWideAsyncHander
```



#### 业务背景

旅游产品订单 数据

```json
{
    "travel_member_adult": "2",
    "travel_member_baby": "0",
    "user_region": "130200",
    "product_fee": "16",
    "product_price": "6",
    "product_traffic_grade": "20",
    "travel_member_yonger": "1",
    "product_traffic_type": "01",
    "product_pub": "210759642|2a6f73f4",
    "order_ct": "1581306968000",
    "KAFKA_ID": "2301aa70e79831b4b2329d66fba7ce8c",
    "user_id": "12415",
    "user_mobile": "19906118410",
    "has_activity": "9",
    "product_id": "210759642",
    "product_traffic": "03",
    "order_id": "158130696800021075964212415"
}
```



#### 宽表构造

```
从上面的【旅游产品订单】中可以看出与其相关的维表有【地域维度表】、【旅游产品维度表】、可能还有【酒店维度表】(具体看是否包含在旅游产品中)，在此我们先以【旅游产品维度表】示例看看如何进行宽表构造。
强调一点维表数据量不能很大(当然维表数据量级较大的话会放在hbase之中)！！！
```



##### 广播方式

注释：具体代码参考com.qf.bigdata.realtime.flink.streaming.etl.dw.orders.OrdersWideDetail2ESHandler



###### 维度数据提取

```
对于mysql维表的读取工作，本例是基于Flink-JDBC包来完成的
1 创建读取的mysql数据源(表)的字段类型列表，List[TypeInformation[_]]
2 构造JDBCInputFormat，随后形成[Row]元素形式的数据流DataStream
3 转换为维表对象ProductDimDO
```



查询使用的产品维度信息

下列语句来自QRealTimeConstant（注意：项目里使用 【travel.dim_product1】为测试表）

```scala
val SQL_PRODUCT = s"""
        select
        |product_id,
        |product_level,
        |product_type,
        |departure_code,
        |des_city_code,
        |toursim_tickets_type
        from travel.dim_product
      """.stripMargin
```



mysql数据表字段类型列表

对照上面的sql语句构造了所选字段类型信息

```scala
/**
  * 维表数据表结构信息
  */
object QRealTimeDimTypeInformations {
  //旅游产品表涉及列类型(所选列集合)
  def getProductDimFieldTypeInfos() : List[TypeInformation[_]] = {
    var colTypeInfos :List[TypeInformation[_]] = List[TypeInformation[_]]()
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.INT_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos
  }
}
```



构造JDBCInputFormat

```scala
/**
    * 维度数据加载
    * @param env
    * @param sql
    * @param fieldTypes
    * @return
    */
  def createOffLineDataStream(env: StreamExecutionEnvironment, sql:String, fieldTypes: Seq[TypeInformation[_]]):DataStream[Row] = {
    //JDBC属性
    val mysqlDBProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.MYSQL_CONFIG_URL)
    val jdbcInputFormat : JDBCInputFormat= FlinkHelper.createJDBCInputFormat(mysqlDBProperties, sql, fieldTypes)
    val jdbcDataStream :DataStream[Row] = env.createInput(jdbcInputFormat)
    jdbcDataStream
  }
```



维度数据提取转换

```scala
/**
 * 2 离线维度数据提取
 *   旅游产品维度数据
 */
val productDimFieldTypes :List[TypeInformation[_]] = QRealTimeDimTypeInformations.getProductDimFieldTypeInfos()
    
//mysql查询sql
val sql = QRealTimeConstant.SQL_PRODUCT
val productDS :DataStream[ProductDimDO] = FlinkHelper.createOffLineDataStream(env, sql, productDimFieldTypes).map(
      (row: Row) => {
        val productID = row.getField(0).toString
        val productLevel = row.getField(1).toString.toInt
        val productType = row.getField(2).toString
        val depCode = row.getField(3).toString
        val desCode = row.getField(4).toString
        val toursimType = row.getField(5).toString
        new ProductDimDO(productID, productLevel, productType, depCode, desCode, toursimType)
      }
)
```



维表数据形成广播流

```scala
//状态描述对象
val productMSDesc = new MapStateDescriptor[String, ProductDimDO](QRealTimeConstant.BC_PRODUCT, createTypeInformation[String], createTypeInformation[ProductDimDO])
 
//产品维表广播流
val dimProductBCStream :BroadcastStream[ProductDimDO] = productDS.broadcast(productMSDesc)
```





###### 订单实时数据

```
如同在【kafka-Flink消费】中类似，通过对订单业务所对应的kafka topic读取并进行转换形成了订单明细对象OrderDetailData，同时基于事件时间进行窗口数据划分并设置了水位，最终形成订单数据流。
注释：下列代码来自于com.qf.bigdata.realtime.flink.streaming.etl.dw.orders.OrdersWideDetail2ESHandler
```



```scala
/**
 * 4 旅游产品订单数据
 *   读取kafka中的原始明细数据并进行转换操作
 *   设置任务执行并行度setParallelism
 *   指定水位生成和事件时间(如果时间语义是事件时间的话)
 */
val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)  
      
val orderDetailDStream :DataStream[OrderDetailData] = env.addSource(kafkaConsumer)                                        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
                                 .map(new OrderDetailDataMapFun())                                                        .assignTimestampsAndWatermarks(ordersPeriodicAssigner)
orderDetailDStream.print("orderDStream---:") //打印测试

```



###### 广播构造宽表

```scala
/**
 * 旅游产品宽表数据
 * 1 产品维度
 * 2 订单数据
 */
val orderWideDStream :DataStream[OrderWideData] = orderDetailDStream.connect(dimProductBCStream)
      .process(new OrderWideBCFunction(QRealTimeConstant.BC_PRODUCT))
```



构造合成过程

```scala
/**
 * 旅游产品维表广播数据处理函数
 * @param bcName 旅游产品维表广播描述名称
 */
class OrderWideBCFunction(bcName:String) extends BroadcastProcessFunction[OrderDetailData, ProductDimDO, OrderWideData]{

//旅游产品维表广播描述对象
val productMSDesc = new MapStateDescriptor[String,ProductDimDO](bcName, createTypeInformation[String], createTypeInformation[ProductDimDO])

//旅游产品维表数据收集器
var products :Seq[ProductDimDO] = List[ProductDimDO]()

    /**
      * 函数初始化
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
    }

/**
 * 数据处理逻辑
 * @param value 产品订单实时数据
 * @param ctx 广播处理函数
 * @param out 数据输出
 */
override def processElement(value: OrderDetailData, ctx: BroadcastProcessFunction[OrderDetailData, ProductDimDO, OrderWideData]#ReadOnlyContext, out: Collector[OrderWideData]): Unit = {
      //获取广播数据状态
      val productBState :ReadOnlyBroadcastState[String,ProductDimDO] = ctx.getBroadcastState(productMSDesc);

      //从产品订单实时数据中提取【产品ID】匹配广播维表数据
      val orderProductID :String = value.productID
      if(productBState.contains(orderProductID)){
        val productDimDO :ProductDimDO = productBState.get(orderProductID)

        val productLevel = productDimDO.productLevel
        val productType = productDimDO.productType
        val toursimType = productDimDO.toursimType
        val depCode = productDimDO.depCode
        val desCode = productDimDO.desCode

        val orderWide = OrderWideData(value.orderID, value.userID, value.productID, value.pubID,
          value.userMobile, value.userRegion, value.traffic, value.trafficGrade, value.trafficType,
          value.price, value.fee, value.hasActivity,
          value.adult, value.yonger, value.baby, value.ct,
          productLevel, productType, toursimType, depCode, desCode)

        //println(s"""orderWide=${JsonUtil.gObject2Json(orderWide)}""")
        out.collect(orderWide)
      }else{
        //对于未匹配上的实时数据需要有默认值进行补救处理
        val notMatch = "-1"

        val orderWide = OrderWideData(value.orderID, value.userID, value.productID, value.pubID,
          value.userMobile, value.userRegion, value.traffic, value.trafficGrade, value.trafficType,
          value.price, value.fee, value.hasActivity,
          value.adult, value.yonger, value.baby, value.ct,
          notMatch.toInt, notMatch, notMatch, notMatch, notMatch)
        //println(s"""orderWide=${JsonUtil.gObject2Json(orderWide)}""")
        out.collect(orderWide)
      }
    }


//广播维表数据收集
override def processBroadcastElement(value: ProductDimDO, ctx: BroadcastProcessFunction[OrderDetailData, ProductDimDO, OrderWideData]#Context, out: Collector[OrderWideData]): Unit = {
      val productBState :BroadcastState[String, ProductDimDO] = ctx.getBroadcastState(productMSDesc);
      products = products.:+(value)

      val key = value.productID
      productBState.put(key, value);
    }
  }
```



##### 异步IO方式

背景说明

```
不同于广播方式，异步IO方式核心思想是实时读取订单数据，异步方式加载其他资源数据（如mysql），然后和事实流数据整合形成宽表数据。
注释：具体代码参考 com.qf.bigdata.realtime.flink.streaming.etl.dw.orders.OrdersWideAsyncHander
```





###### 订单实时数据

```scala
/**
 * 3 旅游产品订单数据
 *   (1) kafka数据源(原始明细数据)->转换操作
 *   (2) 设置执行任务并行度
 *   (3) 设置水位及事件时间(如果时间语义为事件时间)
 */
//固定范围的水位指定(注意时间单位)
val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      
val orderDetailDStream :DataStream[OrderDetailData] = env.addSource(kafkaConsumer)                                        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
                                 .map(new OrderDetailDataMapFun())                                                        .assignTimestampsAndWatermarks(ordersPeriodicAssigner)

```



###### 维表数据提取

```scala
//单维表处理
val useLocalCache :Boolean = false
val dbPath = QRealTimeConstant.MYSQL_CONFIG_URL
val productDBQuery :DBQuery = createProductDBQuery()
val syncFunc = new DimProductAsyncFunction(dbPath, productDBQuery, useLocalCache)
//1 数据库查询对象封装
//2 要根据实际情况(维表变化频率),结合缓存机制提高处理的效率，另外也可防止意外突发情况(断网、mysql访问无响#应等)
//3 在异步IO中定时同步维度数据并构造宽表
```



数据库查询对象封装

```scala
/**
    * 构造旅游产品数据查询对象
    */
  def createProductDBQuery():DBQuery = {
    val sql = QRealTimeConstant.SQL_PRODUCT
    val schema = QRealTimeConstant.SCHEMA_PRODUCT
    val pk = "product_id";
    val tableProduct = QRealTimeConstant.MYDQL_DIM_PRODUCT

    new DBQuery(tableProduct, schema, pk, sql)
  }
```



数据库查询对象

```scala
/**
    * 数据源信息
    *
    * @param table
    * @param schema
    * @param pk
    * @param sql
    */
  case class DBQuery(table:String, schema:String, pk:String, sql:String)
```



###### 异步IO构造宽表

```
下面代码使用了google cache和redis、阿里数据库连接池Druid、定时调度ScheduledThreadPoolExecutor完成一个定时同步数据并结合缓存数据提供维表信息的功能，同时提供了异步处理功能，因为实现了RichAsyncFunction异步富操作函数，最后结合AsyncDataStream完成异步IO构造宽表。
强调一点：由于这里涉及代码及功能点相对较多，所以下面是主要局部代码，具体代码请参考项目
注释： com.qf.bigdata.realtime.flink.streaming.etl.dw.orders.OrdersWideAsyncHander
```



异步处理函数

```scala
/**
 * 产品相关维表异步读取函数(多表)
*/
class DimProductMAsyncFunction(dbPath:String, dbQuerys:mutable.Map[String,DBQuery]) extends RichAsyncFunction[OrderDetailData,OrderMWideData] {
    //mysql连接
    var pool :DBDruid = _

    //redis连接参数
    val redisIP = "node11"
    val redisPort = 6379
    val redisPass = "qfqf"

    //redis客户端连接
    var redisClient : RedisClient = _
    var redisConn : StatefulRedisConnection[String,String] = _
    var redisCmd: RedisCommands[String, String] = _

    //定时调度
    var scheduled : ScheduledThreadPoolExecutor = _

    /**
      * 重新加载数据库数据
      */
    def reloadDB():Unit ={
      for(entry <- dbQuerys){
        val tableKey = entry._1
        val dbQuery = entry._2
        val dbResult = pool.execSQLJson(dbQuery.sql, dbQuery.schema, dbQuery.pk)
        val key = dbQuery.table
        redisCmd.hmset(key, dbResult)
      }
    }


    /**
      * redis连接初始化
      *
      */
    def initRedisConn():Unit = {
      redisClient = RedisClient.create(RedisURI.create(redisIP, redisPort))
      redisConn = redisClient.connect
      redisCmd = redisConn.sync
      redisCmd.auth(redisPass)
    }


    /*
     * 初始化
     */
    override def open(parameters: Configuration): Unit = {
      //println(s"""MysqlAsyncFunction open.time=${CommonUtil.formatDate4Def(new Date())}""")
      super.open(parameters)

      //数据库配置文件
      val dbProperties = PropertyUtil.readProperties(dbPath)

      val driver = dbProperties.getProperty(TravelConstant.FLINK_JDBC_DRIVER_MYSQL_KEY)
      val url = dbProperties.getProperty(TravelConstant.FLINK_JDBC_URL_KEY)
      val user = dbProperties.getProperty(TravelConstant.FLINK_JDBC_USERNAME_KEY)
      val passwd = dbProperties.getProperty(TravelConstant.FLINK_JDBC_PASSWD_KEY)

      //缓存连接
      pool = new DBDruid(driver, url, user, passwd)

      //redis资源连接初始化
      initRedisConn()

      //数据初始化加载到缓存
      reloadDB()

      //定时更新缓存
      //调度资源
      scheduled  = new ScheduledThreadPoolExecutor(2)
      val initialDelay: Long = 0l
      val period :Long = 60l
      scheduled.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = {
          reloadDB()
        }
      }, initialDelay, period, TimeUnit.SECONDS)
    }


    /**
      * 异步执行
      * @param input
      * @param resultFuture
      */
    override def asyncInvoke(input: OrderDetailData, resultFuture: ResultFuture[OrderMWideData]): Unit = {
      try {
        //println(s"""MysqlAsyncFunction invoke.time=${CommonUtil.formatDate4Def(new Date())}""")
        val orderProductID :String = input.productID
        val pubID :String = input.pubID
        val defValue :String = ""

        /**
          * 产品维表相关数据
          * 使用列：product_id, product_level, product_type, departure_code, des_city_code, toursim_tickets_type
          */
        val productJson :String = redisCmd.hget(QRealTimeConstant.MYDQL_DIM_PRODUCT, orderProductID)
        var productLevel = QRealTimeConstant.COMMON_NUMBER_ZERO_INT
        var productType = defValue
        var depCode = defValue
        var desCode = defValue
        var toursimType = defValue
        if(StringUtils.isNotEmpty(productJson)){
          val productRow : util.Map[String,Object] = JsonUtil.json2object(productJson, classOf[util.Map[String,Object]])
          productLevel = productRow.get("product_level").toString.toInt

          productType = productRow.get("product_type").toString
          depCode = productRow.get("departure_code").toString
          desCode = productRow.get("des_city_code").toString
          toursimType = productRow.get("toursim_tickets_type").toString
        }

        /**
          * 酒店维表相关数据
          */
        val pubJson :String = redisCmd.hget(QRealTimeConstant.MYDQL_DIM_PUB, pubID)
        var pubStar :String = defValue
        var pubGrade :String = defValue
        var isNational :String = defValue
        if(StringUtils.isNotEmpty(pubJson)){
          val pubRow : util.Map[String,Object] = JsonUtil.json2object(pubJson, classOf[util.Map[String,Object]])
          pubStar = pubRow.get("pub_star").toString
          pubGrade = pubRow.get("pub_grade").toString
          isNational = pubRow.get("is_national").toString
        }

        var orderWide = new OrderMWideData(input.orderID, input.userID, orderProductID, input.pubID,
          input.userMobile, input.userRegion, input.traffic, input.trafficGrade, input.trafficType,
          input.price, input.fee, input.hasActivity,
          input.adult, input.yonger, input.baby, input.ct,
          productLevel, productType, toursimType, depCode, desCode,
          pubStar, pubGrade, isNational)

        val orderWides = List(orderWide)
        resultFuture.complete(orderWides)
      }catch {
        //注意：加入异常处理放置阻塞产生
        case ex: Exception => {
          println(s"""ex=${ex}""")
          logger.error("DimProductMAsyncFunction.err:" + ex.getMessage)
          resultFuture.completeExceptionally(ex)
        }
      }
    }


    /**
      * 超时处理
      * @param input
      * @param resultFuture
      */
    override def timeout(input: OrderDetailData, resultFuture: ResultFuture[OrderMWideData]): Unit = {
      println(s"""DimProductMAsyncFunction timeout.time=${CommonUtil.formatDate4Def(new Date())}""")
      super.timeout(input, resultFuture)
    }

    /**
      * 关闭
      */
    override def close(): Unit = {
      println(s"""DimProductMAsyncFunction close.time=${CommonUtil.formatDate4Def(new Date())}""")
      super.close()
      pool.close()
    }

  }
```



异步流调用

```scala
val useLocalCache :Boolean = false
val dbPath = QRealTimeConstant.MYSQL_CONFIG_URL
val productDBQuery :DBQuery = createProductDBQuery()
val syncFunc = new DimProductAsyncFunction(dbPath, productDBQuery, useLocalCache)

//异步流处理
val asyncDS :DataStream[OrderWideData] = AsyncDataStream.unorderedWait(orderDetailDStream, syncFunc, QRealTimeConstant.DYNC_DBCONN_TIMEOUT, TimeUnit.MINUTES, QRealTimeConstant.DYNC_DBCONN_CAPACITY)

```



#### 多维表宽表构造

```
对于需要使用多维表来构造宽表的情况，可以参考OrdersWideAsyncHander中的方法，原理和异步IO里提到的类似，核心想法是构造map结构的多维表数据信息后定时查询同步。
强调一点维表数据量不能很大(当然维表数据量级较大的话会放在hbase之中)
```



### 第七节 实时输出

#### 背景说明

```
我们之前说过，对于在高并发或需要处理的业务逻辑较复杂的情况下，对于实时场景的数据计算处理性能产生一定影响，另外如果使用方需求查询实时明细数据或是其他的分组聚合方式，我们都是统计结果输出如何满足需求呢。所以在一些场景下需要我们将实时数据保存起来，当然是经过ETL之后的干净数据。
在实时场景下保存数据又可能会被使用方查询，那么选择时序库是比较好的方案，比如ES和 apache druid(当然druid并不是由Flink直接写入数据，而是通过将数据发送到kafka由druid作为数据源摄取)，这样在Flink的sink端我们介绍下这几种sink，另外作为聚合数据也可以输出到kafka、es、redis之中。项目中根据不同业务需求会有很多类似的开发代码，下面示例仅仅作为展示示例，更多示例请参考项目代码，分别展示了输出数据到kafka和es
1 自定义sink(ES)  
2 输出到kafka，也就是kafka producer
3 输出到redis

注释：
1 参考com.qf.bigdata.realtime.flink.streaming.etl.ods.UserLogsViewHandler分别列举了以上2种sink(kafka,es)。
2 参考com.qf.bigdata.realtime.flink.streaming.agg.orders.OrdersAggCacheHandler将统计结果输出到redis
```



#### 业务背景

##### 用户行为日志之页面浏览数据

创建页面浏览数据流

```
通过kafka数据通道读取全部行为日志经过过滤、ETL处理得到【页面浏览数据】
1 设置数据流的时间语义：事件时间
2 通过kafka配置文件消费消息通道数据
3 通过kafka数据反序列化产生行为日志数据流
4 使用过滤、ETL形成页面浏览日志数据流
5.1 自定义es-sink输出数据(flink有三方的es连接器，但如果你想使用es局部更新功能就需要使用自定义方式)
5.2 作为kafka producer将数据重新打回kafka(当然是不同topic)
```



```scala
/**
    * 用户行为日志(页面浏览行为数据)实时明细数据ETL处理
    * @param appName 程序名称
    * @param fromTopic 数据源输入 kafka topic
    * @param groupID 消费组id
    * @param indexName 输出ES索引名称
    */
def handleLogsETL4ESJob(appName:String, groupID:String, fromTopic:String, indexName:String):Unit = {
try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      //注意：检查点时间间隔单位：毫秒
      val checkpointInterval = QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL
      val tc = TimeCharacteristic.EventTime
      val watermarkInterval= QRealTimeConstant.FLINK_WATERMARK_INTERVAL
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(checkpointInterval, tc, watermarkInterval)


      /**
        * 2 kafka流式数据源
        *   kafka消费配置参数
        *   kafka消费策略
        *   创建flink消费对象FlinkKafkaConsumer
        *   用户行为日志(kafka数据)反序列化处理
        */
      val schema:KafkaDeserializationSchema[UserLogData] = new UserLogsKSchema(fromTopic)
      val kafkaConsumer : FlinkKafkaConsumer[UserLogData] = FlinkHelper.createKafkaSerDeConsumer(env, fromTopic, groupID, schema, StartupMode.LATEST)


      /**
        * 3 设置事件时间提取器及水位计算
        *   方式：自定义实现AssignerWithPeriodicWatermarks 如 UserLogsAssigner
        */
      val userLogsPeriodicAssigner = new UserLogsAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      val viewDStream :DataStream[UserLogPageViewData] = env.addSource(kafkaConsumer)
        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
        .assignTimestampsAndWatermarks(userLogsPeriodicAssigner)
        .filter(
          (log : UserLogData) => {
            log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_H5.getCode) || log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_NATIVE.getCode)
          }
        ).map(new UserLogPageViewDataMapFun())
      viewDStream.print("=====viewDStream========")


      //4 写入下游环节ES(具体下游环节取决于平台的技术方案和相关需求,如flink+es技术组合)
      val viewESSink = new UserLogsViewESSink(indexName)
      viewDStream.addSink(viewESSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("UserLogsViewHandler.err:" + ex.getMessage)
      }
  }

}
```



###### 页面浏览数据

```scala
/**
    *用户行为日志页面浏览操作数据
    */
case class UserLogPageViewData(sid:String, userDevice:String, userDeviceType:String, 		os:String,userID:String,userRegion:String, userRegionIP:String, lonitude:String, 		latitude:String,manufacturer:String, carrier:String, networkType:String,             	 duration:String,action:String, eventType:String, ct:Long, targetID:String)
```



###### 自定义ES-Sink

```scala
/**
  * 用户行为日志页面浏览明细数据输出ES
  */
class UserLogsViewESSink(indexName:String) extends RichSinkFunction[UserLogPageViewData]{


  //日志记录
  val logger :Logger = LoggerFactory.getLogger(this.getClass)

  //ES客户端连接对象
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
  override def invoke(value: UserLogPageViewData, context: SinkFunction.Context[_]): Unit = {
    try {
      //参数校验
      val result :java.util.Map[String,Object] = JsonUtil.gObject2Map(value)
      val checkResult: String = checkData(result)
      if (StringUtils.isNotBlank(checkResult)) {
        //日志记录
        logger.error("Travel.ESRecord.sink.checkData.err{}", checkResult)
        return
      }


      //请求id
      val sid = value.sid

      //索引名称、类型名称
      handleData(indexName, indexName, sid, result)

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
  def handleData(idxName :String, idxTypeName :String, esID :String,
                 value: java.util.Map[String,Object]): Unit ={
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
  def checkData(value :java.util.Map[String,Object]): String ={
    var msg = ""
    if(null == value){
      msg = "kafka.value is empty"
    }

    //行为类型
    val action = value.get(QRealTimeConstant.KEY_ACTION)
    if(null == action){
      msg = "Travel.ESSink.action  is null"
    }

    //行为类型
    val eventTyoe = value.get(QRealTimeConstant.KEY_EVENT_TYPE)
    if(null == eventTyoe){
      msg = "Travel.ESSink.eventtype  is null"
    }

    //时间
    val ctNode = value.get(TravelConstant.CT)
    if(null == ctNode){
      msg = "Travel.ESSink.ct is null"
    }

    msg
  }


}
```



###### 加工数据重发kafka

kafka反序列化

```scala
/**
  * 行为日志页面浏览数据(明细)kafka序列化
  */
class UserLogsPageViewKSchema (topic:String) extends KafkaSerializationSchema[UserLogPageViewData] with KafkaDeserializationSchema[UserLogPageViewData]{

  /**
    * 反序列化
    * @param message
    * @return
    */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogPageViewData = {
    val key = record.key()
    val value = record.value()
    val gson : Gson = new Gson()  
    val log :UserLogPageViewData = gson.fromJson(new String(value), classOf[UserLogPageViewData])
    log
  }

  /**
    * 序列化
    * @param element
    * @return
    */
  override def serialize(element: UserLogPageViewData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val sid = element.sid
    val userDevice = element.userDevice
    val targetID = element.targetID
    val tmp = sid + userDevice+ targetID
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)

    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }

  override def isEndOfStream(nextElement: UserLogPageViewData): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[UserLogPageViewData] = {
    return TypeInformation.of(classOf[UserLogPageViewData])
  }


}
```



Flink kafka生产者

```scala
val kafkaSerSchema :KafkaSerializationSchema[UserLogPageViewData] = new UserLogsPageViewKSchema(toTopic)

val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
    val viewKafkaProducer = new FlinkKafkaProducer(
      toTopic,
      kafkaSerSchema,
      kafkaProductConfig,
      FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
    viewKafkaProducer.setWriteTimestampToKafka(true)

viewDStream.addSink(viewKafkaProducer)
```





### 第八节 实时数据统计

#### 背景说明

``` 
在实际工作中，不论离线还是实时场景统计报表、统计排名、去重统计是常用的计算方式，再细分又有开窗统计还是实时统计，其中就涉及开窗方式、窗口计算触发方式、延迟到达数据、测流输出、数据流状态保存等问题，下面我们来一一解决。
```



#### 基于窗口数据的统计

##### 订单事实数据统计

###### 需求点

```
解决以【userRegion:用户所属地区、traffic:出游交通方式】做维度，
【orders:订单数量, maxFee:最高消费, totalFee:消费总数, members:旅游人次】做度量
旅游订单固定间隔N分钟统计指标或近N分钟统计指标。
```

###### 技术分析

```
1 数据源
  基于订单事实明细数据来完成
  
2 结果为时间导向的计算需求，考虑以事件时间窗口进行统计计算 
  基于时间的滚动窗口(TumblingEventTimeWindows)统计，如每N分钟统计结果

3 数据输出
  如果结果要实时展示，那么可以结合grafana做展示，那么输出数据要输出到es、druid、redis之中，当然它们也支持客户端交互式查询需求并有较好的性能。
  如果要高效的交互式查询，除了上面几种方案，也可以输出到clickhouse
```



窗口统计过程(局部代码)

详细代码参考：com.qf.bigdata.realtime.flink.streaming.agg.orders.OrdersDetailAggHandler

```scala
/**
 * 3 开窗聚合操作
 * (1) 分组维度列：用户所在地区(userRegion),出游交通方式(traffic)
 * (2) 聚合结果数据(分组维度+度量值)：OrderDetailTimeAggDimMeaData
 * (3) 开窗方式：滚动窗口TumblingEventTimeWindows
 * (4) 允许数据延迟：allowedLateness
 * (5) 聚合计算方式：aggregate
 */
 val aggDStream:DataStream[OrderDetailTimeAggDimMeaData] = orderDetailDStream
     .keyBy(
        (detail:OrderDetailData) => 
         OrderDetailAggDimData(detail.userRegion, detail.traffic)
      )               .window(TumblingEventTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE)))
        .allowedLateness(Time.seconds(QRealTimeConstant.FLINK_ALLOWED_LATENESS))
        .aggregate(new OrderDetailTimeAggFun(), new OrderDetailTimeWindowFun())
aggDStream.print("order.aggDStream  ---:")
```



聚合函数实现OrderDetailTimeAggFun

```scala
/**
    * 订单时间窗口预聚合函数
    */
  class OrderDetailTimeAggFun extends AggregateFunction[OrderDetailData, OrderDetailTimeAggMeaData, OrderDetailTimeAggMeaData] {
    /**
      * 创建累加器
      */
    override def createAccumulator(): OrderDetailTimeAggMeaData = {
      OrderDetailTimeAggMeaData(QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO,
        QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO)
    }

    /**
      * 累加开始
      */
    override def add(value: OrderDetailData, accumulator: OrderDetailTimeAggMeaData): OrderDetailTimeAggMeaData = {
      val orders = QRealTimeConstant.COMMON_NUMBER_ONE + accumulator.orders
      var maxFee = accumulator.maxFee.max(value.fee)
      val totalFee = value.fee + accumulator.totalFee
      val orderMembers = value.adult.toInt + value.yonger.toInt + value.baby.toInt
      val members = orderMembers + accumulator.members

      OrderDetailTimeAggMeaData(orders, maxFee, totalFee, members)
    }

    /**
      * 获取结果数据
      */
    override def getResult(accumulator: OrderDetailTimeAggMeaData): OrderDetailTimeAggMeaData = {
      accumulator
    }

    /**
      * 合并中间数据
      */
    override def merge(a: OrderDetailTimeAggMeaData, b: OrderDetailTimeAggMeaData): OrderDetailTimeAggMeaData = {
      val orders = a.orders + a.orders
      var maxFee = a.maxFee.max(b.maxFee)
      val totalFee = a.totalFee + b.totalFee
      val members = a.members + b.members
      OrderDetailTimeAggMeaData(orders, maxFee, totalFee, members)
    }
  }
```



窗口函数输出统计结果

```scala
/**
    * 订单时间窗口输出函数
    */
  class OrderDetailTimeWindowFun extends WindowFunction[OrderDetailTimeAggMeaData, OrderDetailTimeAggDimMeaData, OrderDetailAggDimData, TimeWindow]{
    override def apply(key: OrderDetailAggDimData, window: TimeWindow, input: Iterable[OrderDetailTimeAggMeaData], out: Collector[OrderDetailTimeAggDimMeaData]): Unit = {
      //分组维度
      val userRegion = key.userRegion
      val traffic = key.traffic

      //度量计算
      var outOrders = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMaxFee = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outFees = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMembers = QRealTimeConstant.COMMON_NUMBER_ZERO
      for(meaData: OrderDetailTimeAggMeaData <- input){
        outOrders = outOrders + meaData.orders
        outMaxFee = outMaxFee.max(meaData.maxFee)
        outFees = outFees + meaData.totalFee
        outMembers = outMembers + meaData.members
      }
      val outAvgFee = outFees / outOrders

      //窗口时间
      val startWindowTime = window.getStart
      val endWindowTime = window.getEnd

      val owDMData = OrderDetailTimeAggDimMeaData(userRegion, traffic, startWindowTime, endWindowTime,outOrders, outMaxFee, outFees, outMembers, outAvgFee)
      out.collect(owDMData)
    }
  }
```



###### 其他Sink选择

窗口统计结果输出redis

备注：

为了不对OrdersDetailAggHandler里的代码产生过多的混乱性，单独写了一个redis sink的类OrdersAggCacheHandler（具体代码参考：com.qf.bigdata.realtime.flink.streaming.agg.orders.OrdersAggCacheHandler）



redis连接参数

```scala
/**
 * redis连接参数(单点)
 */
def createRedisConfig() : FlinkJedisConfigBase = {

//redis配置文件
val redisProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.REDIS_CONF_PATH)

//redis连接参数
val redisDB :Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_DB).toInt
val redisMaxIdle :Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_MAXIDLE).toInt
val redisMinIdle:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_MINIDLE).toInt
val redisMaxTotal:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_MAXTOTAL).toInt
val redisHost:String = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_HOST)
val redisPassword:String = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_PASSWORD)
val redisPort:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_PORT).toInt
val redisTimeout:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_TIMEOUT).toInt

//redis配置对象构造
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
```



RedisMapper接口实现

```scala
/**
  * 自定义实现redis sink的RedisMapper接口
  * 订单聚合结果输出redis
  * @param redisCommand
  */
class QRedisSetMapper() extends RedisMapper[QKVBase]{

  /**
    * redis 操作命令
    * @return
    */
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET, null)
  }

  /**
    * redis key键
    * @param data
    * @return
    */
  override def getKeyFromData(data: QKVBase): String = {
    data.key
  }

  /**
    * redis value
    * @param data
    * @return
    */
  override def getValueFromData(data: QKVBase): String = {
      data.value
  }
}
```



写入redis数据

```scala
//订单统计结果(QKVBase为kv结构，使用Tuple也可)
val aggDStream:DataStream[QKVBase] = ...

/**
 * 4 写入下游环节缓存redis(被其他系统调用)
 */
val redisMapper = new QRedisSetMapper()
val redisConf = FlinkHelper.createRedisConfig()
val redisSink = new RedisSink(redisConf, redisMapper)
aggDStream.addSink(redisSink)
```







##### 订单宽表统计

###### 需求点

```
解决以【productType:产品种类、toursimType:产品类型】做维度，【orders:订单数量, maxFee:最高消费, totalFee:消费总数, members:旅游人次】做度量的订单固定间隔N分钟统计指标或近N分钟统计指标。
```

###### 技术分析

```
1 数据源
  考虑到聚合维度的扩展性，可以基于订单宽表明细数据来完成而不是订单事实数据
  
2 结果为时间导向的计算需求，考虑以处理时间窗口（TumblingProcessingTimeWindows）进行统计计算 

3 数据输出
  如果结果要实时展示，那么可以结合grafana做展示，那么输出数据要输出到es、druid之中，当然它们也支持客户端交互式查询需求并有较好的性能。
  如果要高效的交互式查询，除了上面2种方案，也可以输出到clickhouse
```



窗口统计过程(局部代码)

详细代码参考：com.qf.bigdata.realtime.flink.streaming.agg.orders.OrdersWideTimeAggHandler

```scala
/**
 * 7 基于订单宽表数据的聚合统计
 *  (1) 分组维度：产品类型(productType)+出境类型(toursimType)
 *  (2) 开窗方式：基于时间的滚动窗口
 *  (3) 数据处理函数：aggregate
 */
val aggDStream:DataStream[OrderWideTimeAggDimMeaData] = orderWideGroupDStream
        .keyBy({
          (wide:OrderWideData) => OrderWideAggDimData(wide.productType, wide.toursimType)
        })      .window(TumblingProcessingTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE)))
        .aggregate(new OrderWideTimeAggFun(), new OrderWideTimeWindowFun())
 aggDStream.print("order.aggDStream---:")
```



聚合函数aggregate之AggregateFunction实现

```scala
/**
 * 订单宽表时间窗口预聚合函数
 */
class OrderWideTimeAggFun extends AggregateFunction[OrderWideData, OrderWideTimeAggMeaData, OrderWideTimeAggMeaData] {
    /**
      * 创建累加器
      */
    override def createAccumulator(): OrderWideTimeAggMeaData = {
      OrderWideTimeAggMeaData(QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO,
        QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO)
    }

    /**
      * 累加开始
      */
    override def add(value: OrderWideData, accumulator: OrderWideTimeAggMeaData): OrderWideTimeAggMeaData = {
      val orders = QRealTimeConstant.COMMON_NUMBER_ONE + accumulator.orders
      var maxFee = accumulator.maxFee.max(value.fee)
      val totalFee = value.fee + accumulator.totalFee
      val orderMembers = value.adult.toInt + value.yonger.toInt + value.baby.toInt
      val members = orderMembers + accumulator.members
      OrderWideTimeAggMeaData(orders, maxFee, totalFee, members)
    }

    /**
      * 获取结果数据
      */
    override def getResult(accumulator: OrderWideTimeAggMeaData): OrderWideTimeAggMeaData = {
      accumulator
    }

    /**
      * 合并中间数据
      */
    override def merge(a: OrderWideTimeAggMeaData, b: OrderWideTimeAggMeaData): OrderWideTimeAggMeaData = {
      val orders = a.orders + a.orders
      var maxFee = a.maxFee.max(b.maxFee)
      val totalFee = a.totalFee + b.totalFee
      val members = a.members + b.members
      OrderWideTimeAggMeaData(orders, maxFee, totalFee, members)
    }
  }
```



聚合函数aggregate之WindowFunction实现

```scala
/**
 * 订单宽表时间窗口输出函数
 */
class OrderWideTimeWindowFun extends WindowFunction[OrderWideTimeAggMeaData, OrderWideTimeAggDimMeaData, OrderWideAggDimData, TimeWindow]{
    override def apply(key: OrderWideAggDimData, window: TimeWindow, input: Iterable[OrderWideTimeAggMeaData], out: Collector[OrderWideTimeAggDimMeaData]): Unit = {
      //分组维度
      val productType = key.productType
      val toursimType = key.toursimType

      //度量计算
      var outOrders = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMaxFee = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outFees = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMembers = QRealTimeConstant.COMMON_NUMBER_ZERO
      for(meaData :OrderWideTimeAggMeaData <- input){
        outOrders = outOrders + meaData.orders
        outMaxFee = outMaxFee.max(meaData.maxFee)
        outFees = outFees + meaData.totalFee
        outMembers = outMembers + meaData.members
      }
      val outAvgFee = outFees / outOrders

      //窗口时间
      val startWindowTime = window.getStart
      val endWindowTime = window.maxTimestamp()

      val owDMData = OrderWideTimeAggDimMeaData(productType, toursimType, startWindowTime, endWindowTime,outOrders, outMaxFee, outFees, outMembers, outAvgFee)
      out.collect(owDMData)
    }
  }
```



统计结果输出kafka供下游处理

```scala
//kafka数据序列化
val orderWideGroupKSerSchema = new OrderWideGroupKSchema(toTopic)

//kafka配置信息
val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)

//kafka生产者
val travelKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        orderWideGroupKSerSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

//5 加入kafka摄入时间
travelKafkaProducer.setWriteTimestampToKafka(true)
aggDStream.addSink(travelKafkaProducer)
```



##### 订单统计排名

###### 需求点

```
有了分类统计结果就可以进行排名操作，比如各种热门排名
```

###### 技术分析

```
1 数据源
  订单统计结果
  
2 窗口内的数据处理(ProcessWindowFunction)，排序比较逻辑，采用列表状态维护排名topN

3 数据输出
  如果结果要实时展示，那么可以结合grafana做展示，那么输出数据要输出到es、druid之中，当然它们也支持客户端交互式查询需求并有较好的性能。
  如果要高效的交互式查询，除了上面2种方案，也可以输出到clickhouse
```



窗口统计过程(局部代码)

详细代码参考：com.qf.bigdata.realtime.flink.streaming.agg.orders.OrdersTopnStatisHandler

```scala
/**
 * 4 topN排序
 * Sorted的数据结构TreeSet或者是优先级队列PriorityQueue
 * (1) TreeSet实现原理是红黑树,时间复杂度是logN
 * (2) 优先队列实现原理就是最大/最小堆,堆的构造复杂度是N, 读取复杂度是1
 * 目前场景对应的是写操作频繁那么选择红黑树结构相对较好
 */
 val topNDStream:DataStream[OrderTrafficDimMeaData] = aggDStream.keyBy(
        (detail:OrderTrafficDimMeaData) => {
          OrderTrafficDimData(detail.productID, detail.traffic)
        }
      )    .window(TumblingEventTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE * 3)))
.allowedLateness(Time.seconds(QRealTimeConstant.FLINK_ALLOWED_LATENESS))
.process(new OrderTopNKeyedProcessFun(topN))

topNDStream.print("order.topNDStream---:")

```



窗口处理函数ProcessWindowFunction

```scala
/**
 * 订单统计排名的窗口处理函数
 */
class OrderTopNKeyedProcessFun(topN:Long) extends ProcessWindowFunction[OrderTrafficDimMeaData, OrderTrafficDimMeaData, OrderTrafficDimData, TimeWindow] {
  /**
    * 处理数据
    */
    override def process(key: OrderTrafficDimData, context: Context, elements: Iterable[OrderTrafficDimMeaData], out: Collector[OrderTrafficDimMeaData]): Unit = {
      //原始数据
      val productID = key.productID
      val traffic = key.traffic

      //排序比较(基于TreeSet)
      val topNContainer = new java.util.TreeSet[OrderTrafficDimMeaData](new OrderTopnComparator())

      for(element :OrderTrafficDimMeaData <- elements){
         val orders = element.orders
         val totalFee = element.totalFee
         val startWindowTime = element.startWindowTime
         val endWindowTime = element.endWindowTime

         val value = new OrderTrafficDimMeaData(productID, traffic, startWindowTime, endWindowTime,orders, totalFee)
         if(!topNContainer.isEmpty){
           if(topNContainer.size() >= topN){
             val first : OrderTrafficDimMeaData = topNContainer.first()
             val result = topNContainer.comparator().compare(first, value)
             if(result < 0){
               topNContainer.pollFirst()
               topNContainer.add(value)
             }
           }else{
             topNContainer.add(value)
           }
         }else{
           topNContainer.add(value)
         }
       }

       for(data <- topNContainer){
         out.collect(data)
       }
    }
  }
```



##### 订单统计(条件触发器)

###### 需求点

```
1 数据源
  基于订单事实明细数据来完成
  
2 结果为时间导向的计算需求，考虑以时间窗口进行统计计算，但如果时间周期相对较长时（30分钟以上），此时又有需求要求在此期间输出数据，表现为特定条件时触发输出(大家可以回想下采集框架flume) 如数据量>m，时间间隔输出
  基于处理时间的滚动窗口(TumblingProcessingTimeWindows)统计，如每N分钟统计结果（实际事件发生时间）

3 数据输出
  如果结果要实时展示，那么可以结合grafana做展示，那么输出数据要输出到es、druid之中，当然它们也支持客户端交互式查询需求并有较好的性能。
  如果要高效的交互式查询，除了上面2种方案，也可以输出到clickhouse
```



###### 技术分析

```
1 数据源
  订单明细数据
  
2 根据需求来看仅仅依靠Flink提供的默认窗口方式不足以满足(本质是内置的窗口触发器)，故需要自定义实现触发器来满足数据量和时间条件上的先后关系Trigger

```



窗口统计过程(局部代码)

详细代码参考：com.qf.bigdata.realtime.flink.streaming.agg.orders.OrdersStatisHandler



- 数量触发：OrdersStatisHandler的handleOrdersStatis4CountJob


```scala
 /**
   * 3 基于订单数据量触发的订单统计
   *  (1) 分组维度：小时粒度的时间维度(hourTime)+出游交通方式(traffic)
   *      下面注释的地方为另一种小时时间提取方法
   *  (2) 开窗方式：基于处理时间的滚动窗口
   *  (3) 窗口触发器：自定义基于订单数据量的窗口触发器OrdersStatisCountTrigger
   *  (4) 数据处理函数：OrderStatisWindowProcessFun
  */
 val statisDStream:DataStream[OrderDetailStatisData] = orderDetailDStream.keyBy(
        (detail:OrderDetailData) => {
          //val hourTime2 = TimeWindow.getWindowStartWithOffset(detail.ct, 0, Time.minutes(30).toMilliseconds)
          //println(s"""hourTime2=${hourTime2}, hourTime2Str=${CommonUtil.formatDate4Timestamp(hourTime2, QRealTimeConstant.FORMATTER_YYYYMMDDHHMMSS)}""")
          val hourTime = CommonUtil.formatDate4Timestamp(detail.ct, QRealTimeConstant.FORMATTER_YYYYMMDDHH)
          OrderDetailSessionDimData(detail.traffic, hourTime)
        }
      )    .window(TumblingEventTimeWindows.of(Time.minutes(QRealTimeConstant.FLINK_WINDOW_MAX_SIZE)))
.trigger(new OrdersStatisCountTrigger(maxCount))
.process(new OrderStatisWindowProcessFun())
```



自定义触发器

```scala
/**
  * 旅游产品订单业务自定义数量触发器
  * 基于计数器触发任务处理
  */
class OrdersStatisCountTrigger(maxCount:Long) extends Trigger[OrderDetailData, TimeWindow]{

  val logger :Logger = LoggerFactory.getLogger("OrdersStatisCountTrigger")

  //统计数据状态：计数
  val TRIGGER_ORDER_STATE_ORDERS_DESC = "TRIGGER_ORDER_STATE_ORDERS_DESC"
  var ordersCountStateDesc :ValueStateDescriptor[Long] = new ValueStateDescriptor[Long](TRIGGER_ORDER_STATE_ORDERS_DESC, createTypeInformation[Long])
  var ordersCountState :ValueState[Long] = _



  /**
    * 每条数据被添加到窗口时调用
    * @param element
    * @param timestamp
    * @param window
    * @param ctx
    * @return
    */
  override def onElement(element: OrderDetailData, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //计数状态
    ordersCountState = ctx.getPartitionedState(ordersCountStateDesc)

    //当前数据
    if(ordersCountState.value() == null){
        ordersCountState.update(QRealTimeConstant.COMMON_NUMBER_ZERO)
    }
    val curOrders = ordersCountState.value() + 1
    ordersCountState.update(curOrders)

    //触发条件判断
    if(curOrders >= maxCount ){
      this.clear(window, ctx)
      return TriggerResult.FIRE_AND_PURGE
    }

    return TriggerResult.CONTINUE
  }




  /**
    * ,当一个已注册的事件时间计时器启动时调用
    * @param time
    * @param window
    * @param ctx
    * @return
    */
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.CONTINUE;
  }

  /**
    * 一个已注册的处理时间计时器启动时调用
    * @param time
    * @param window
    * @param ctx
    * @return
    */
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.FIRE
  }

  /**
    * 执行任何需要清除的相应窗口
    * @param window
    * @param ctx
    */
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    //计数清零
    ctx.getPartitionedState(ordersCountStateDesc).clear()

    //删除处理时间的定时器
    ctx.deleteProcessingTimeTimer(window.maxTimestamp())
  }

  override def canMerge: Boolean = {return true}

  /**
    * 合并窗口
    * @param window
    * @param ctx
    */
  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    println(s"""OrdersStatisCountTrigger.onMerge=${CommonUtil.formatDate4Def(new Date())}""")

    val windowMaxTimestamp :Long = window.maxTimestamp();
    if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
      ctx.registerProcessingTimeTimer(windowMaxTimestamp);
    }
  }
}

```



- 时间触发：OrdersStatisHandler的handleOrdersStatis4ProcceTimeJob


```scala
 /**
  * 3 基于处理触发的订单统计
  *  (1) 分组维度：小时粒度的时间维度(hourTime)+出游交通方式(traffic)
  *  (2) 开窗方式：基于处理时间的滚动窗口
  *  (3) 窗口触发器：自定义基于订单数据量的窗口触发器OrdersStatisTimeTrigger
  *  (4) 数据处理函数：OrderStatisWindowProcessFun
  */
 val statisDStream:DataStream[OrderDetailStatisData] = orderDetailDStream.keyBy(
        (detail:OrderDetailData) => {
          val hourTime = CommonUtil.formatDate4Timestamp(detail.ct, QRealTimeConstant.FORMATTER_YYYYMMDDHH)
          OrderDetailSessionDimData(detail.traffic, hourTime)
        }
      )    .window(TumblingProcessingTimeWindows.of(Time.days(QRealTimeConstant.COMMON_NUMBER_ONE),  Time.hours(-8))) //每天5分钟触发
.trigger(new OrdersStatisTimeTrigger(maxInternal, TimeUnit.MINUTES))
.process(new OrderStatisWindowProcessFun())
```



自定义触发器

```scala
**
  * 旅游产品订单业务自定义时间触发器
  * 基于处理时间间隔时长触发任务处理
  */
class OrdersStatisTimeTrigger(maxInterval :Long, timeUnit:TimeUnit) extends Trigger[OrderDetailData, TimeWindow]{

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("OrdersStatisTimeTrigger")

  //订单统计业务的处理时间状态
  val TRIGGER_ORDER_STATE_TIME_DESC = "TRIGGER_ORDER_STATE_TIME_DESC"
  var ordersTimeStateDesc :ValueStateDescriptor[Long] = new ValueStateDescriptor[Long](TRIGGER_ORDER_STATE_TIME_DESC, createTypeInformation[Long])
  var ordersTimeState :ValueState[Long] = _


  /**
    * 元素处理
    * @param element 数据类型
    * @param timestamp 元素时间
    * @param window 窗口
    * @param ctx 上下文环境
    * @return
    */
  override def onElement(element: OrderDetailData, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //计数状态
    ordersTimeState = ctx.getPartitionedState(ordersTimeStateDesc)

    //处理时间间隔
    val maxIntervalTimestamp :Long = Time.of(maxInterval,timeUnit).toMilliseconds
    val curProcessTime :Long = ctx.getCurrentProcessingTime

    //当前处理时间到达或超过上次处理时间+间隔后触发本次窗口操作
    var nextProcessingTime = TimeWindow.getWindowStartWithOffset(curProcessTime, 0, maxIntervalTimestamp) + maxIntervalTimestamp
    ctx.registerProcessingTimeTimer(nextProcessingTime)

    ordersTimeState.update(nextProcessingTime)
    return TriggerResult.CONTINUE
  }

  /**
    * 一个已注册的处理时间计时器启动时调用
    * @param time
    * @param window
    * @param ctx
    * @return
    */
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.FIRE;
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.CONTINUE;
  }

  /**
    * 执行任何需要清除的相应窗口
    * @param window
    * @param ctx
    */
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    //删除处理时间的定时器
    ctx.deleteProcessingTimeTimer(ordersTimeState.value())
  }

  /**
    * 合并窗口
    * @param window
    * @param ctx
    */
  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    val windowMaxTimestamp = window.maxTimestamp()
    if(windowMaxTimestamp > ctx.getCurrentWatermark){
      ctx.registerProcessingTimeTimer(windowMaxTimestamp)
    }
  }


}
```





##### 订单统计(PUV)

###### 需求点

```
解决以【traffic:出游交通方式、时间维度 小时级 hourTime】做维度，【orders:订单数量, maxFee:最高消费, totalFee:消费总数, members:旅游人次】做度量的订单固定间隔N分钟统计指标或近N分钟统计指标。
关键点：UV（去重）
```

###### 技术分析

```
1 数据源
  基于订单事实明细数据来完成
  
2 结果为时间导向的计算需求，考虑以时间窗口进行统计计算 
  基于处理时间的滚动窗口(TumblingProcessingTimeWindows)统计，如每N分钟统计结果（实际事件发生时间）
  关键是：UV如何解决，需要维护数据状态

3 数据输出
  如果结果要实时展示，那么可以结合grafana做展示，那么输出数据要输出到es、druid之中，当然它们也支持客户端交互式查询需求并有较好的性能。
  如果要高效的交互式查询，除了上面2种方案，也可以输出到clickhouse
```



窗口统计过程(局部代码)

详细代码参考：com.qf.bigdata.realtime.flink.streaming.agg.orders.OrdersStatisHandler



数据状态维护

```scala
/**
    * 订单统计数据的窗口处理函数
    */
  class OrderStatisWindowProcessFun extends ProcessWindowFunction[OrderDetailData, OrderDetailStatisData, OrderDetailSessionDimData, TimeWindow]{

    //参与订单用户数量的状态描述名称
    val ORDER_STATE_USER_DESC = "ORDER_STATE_USER_DESC"
    var usersState :ValueState[mutable.Set[String]] = _
    var usersStateDesc :ValueStateDescriptor[mutable.Set[String]] = _

    //订单数量的状态描述名称
    val ORDER_STATE_ORDERS_DESC = "ORDER_STATE_ORDERS_DESC"
    var ordersAccState :ValueState[OrderAccData] = _
    var ordersAccStateDesc :ValueStateDescriptor[OrderAccData] = _


    /**
      * 相关连接资源初始化(如果需要)
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {
      //订单用户列表的状态数据
      usersStateDesc = new ValueStateDescriptor[mutable.Set[String]](ORDER_STATE_USER_DESC, createTypeInformation[mutable.Set[String]])
      usersState = this.getRuntimeContext.getState(usersStateDesc)

      //订单状态数据：订单度量(订单总数量、订单总费用)
      ordersAccStateDesc = new ValueStateDescriptor[OrderAccData](ORDER_STATE_ORDERS_DESC, createTypeInformation[OrderAccData])
      ordersAccState = this.getRuntimeContext.getState(ordersAccStateDesc)
    }




    /**
      * 数据处理
      * @param value
      * @param ctx
      * @param out
      */
    override def process(key: OrderDetailSessionDimData, context: Context, elements: Iterable[OrderDetailData], out: Collector[OrderDetailStatisData]): Unit = {
      //订单分组维度：出行方式、事件时间
      val traffic = key.traffic
      val hourTime = key.hourTime

      //时间相关
      val curWatermark :Long = context.currentWatermark
      val ptTime:String = CommonUtil.formatDate4Timestamp(context.currentProcessingTime, QRealTimeConstant.FORMATTER_YYYYMMDDHHMMSS)


      //状态相关数据
      var userKeys :mutable.Set[String] = usersState.value
      var ordersAcc :OrderAccData =  ordersAccState.value
      if(null == ordersAcc){
        ordersAcc = new OrderAccData(QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO)
      }
      if(null == userKeys){
        userKeys = mutable.Set[String]()
      }

      //数据聚合处理
      var totalOrders :Long = ordersAcc.orders
      var totalFee :Long = ordersAcc.totalFee
      var usersCount :Long = userKeys.size
      for(element <- elements){
        //度量数据处理
        val userID = element.userID
        totalOrders +=  1
        totalFee += element.fee

        //UV判断
        if(!userKeys.contains(userID)){
          userKeys += userID
        }
        usersCount = userKeys.size
      }

      val orderDetailStatis = new OrderDetailStatisData(traffic, hourTime, totalOrders, usersCount, totalFee, ptTime)
      //println(s"""orderDetailStatis=${orderDetailStatis}""")
      out.collect(orderDetailStatis)

      //状态数据更新
      usersState.update(userKeys)
      ordersAccState.update(new OrderAccData(totalOrders, totalFee))
    }


    /**
      * 状态数据清理
      * @param context
      */
    override def clear(context: Context): Unit = {
      usersState.clear()
      ordersAccState.clear()
    }


  }
```



#### 实时数据统计(非窗口统计)

##### 订单事实数据统计(自定义处理)

###### 需求点

```
解决以【productType:产品种类、toursimType:产品类型】做维度，【startWindowTime:开始时间, endWindowTime:结束时间, orders:订单数, users:用户人数, totalFee:消费总数】做度量的自定义触发、计算处理。
```

###### 技术分析

```
1 数据源
  基于订单事实明细数据来完成
  
2 结果为实时流式数据自定义窗口数据划分、自定义触发方式、自定义数据处理过程，是Flink提供的最自由、最灵活的方式，理论上解决绝大部分相关需求。

3 数据输出
  如果结果要实时展示，那么可以结合grafana做展示，那么输出数据要输出到es、druid之中，当然它们也支持客户端交互式查询需求并有较好的性能。
  如果要高效的交互式查询，除了上面2种方案，也可以输出到clickhouse
```



窗口统计过程(局部代码)

详细代码参考：com.qf.bigdata.realtime.flink.streaming.agg.orders.OrdersCustomerStatisHandler

```scala
/**
 * 5 基于宽表明细数据进行聚合统计
 *   (1) 分组维度：产品类型(productType)，产品种类(出境|非出境)(toursimType)
 *   (2) 数据处理函数：OrderCustomerStatisKeyedProcessFun
 *      (基于数据量和间隔时间触发进行数据划分统计)
 */
val statisDStream:DataStream[OrderWideCustomerStatisData] = orderWideDStream.keyBy(
    (wide:OrderWideData) => {
        OrderWideAggDimData(wide.productType, wide.toursimType)
    }
)
.process(new OrderCustomerStatisKeyedProcessFun(maxCount, maxInterval, TimeUnit.MINUTES))

```



处理逻辑与状态维护

```scala
/**
    * 订单业务：自定义分组处理函数
    */
  class OrderCustomerStatisKeyedProcessFun(maxCount :Long, maxInterval :Long, timeUnit:TimeUnit) extends KeyedProcessFunction[OrderWideAggDimData, OrderWideData, OrderWideCustomerStatisData] {

    //参与订单的用户数量状态描述名称
    val CUSTOMER_ORDER_STATE_USER_DESC = "CUSTOMER_ORDER_STATE_USER_DESC"
    var customerUserState :ValueState[mutable.Set[String]] = _
    var customerUserStateDesc :ValueStateDescriptor[mutable.Set[String]] = _

    //订单数量状态描述名称
    val CUSTOMER_ORDER_STATE_ORDERS_DESC = "CUSTOMER_ORDER_STATE_ORDERS_DESC"
    var customerOrdersAccState :ValueState[OrderAccData] = _
    var customerOrdersAccStateDesc :ValueStateDescriptor[OrderAccData] = _

    //统计时间范围的状态描述名称
    val CUSTOMER_ORDER_STATE_PROCESS_DESC = "CUSTOMER_ORDER_STATE_PROCESS_DESC"
    var customerProcessState :ValueState[QProcessWindow] = _
    var customerProcessStateDesc :ValueStateDescriptor[QProcessWindow] = _


    /**
      * 初始化
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {

      //状态数据：订单UV
      customerUserStateDesc = new ValueStateDescriptor[mutable.Set[String]](CUSTOMER_ORDER_STATE_USER_DESC, createTypeInformation[mutable.Set[String]])
      customerUserState = this.getRuntimeContext.getState(customerUserStateDesc)

      //状态数据：订单度量(订单总数量、订单总费用)
      customerOrdersAccStateDesc = new ValueStateDescriptor[OrderAccData](CUSTOMER_ORDER_STATE_ORDERS_DESC, createTypeInformation[OrderAccData])
      customerOrdersAccState = this.getRuntimeContext.getState(customerOrdersAccStateDesc)

      //处理时间
      customerProcessStateDesc = new ValueStateDescriptor[QProcessWindow](CUSTOMER_ORDER_STATE_PROCESS_DESC, createTypeInformation[QProcessWindow])
      customerProcessState = this.getRuntimeContext.getState(customerProcessStateDesc)
    }

    /**
      * 处理数据
      * @param value 元素数据
      * @param ctx 上下文环境对象
      * @param out 输出结果
      */
    override def processElement(value: OrderWideData, ctx: KeyedProcessFunction[OrderWideAggDimData, OrderWideData, OrderWideCustomerStatisData]#Context, out: Collector[OrderWideCustomerStatisData]): Unit = {
      //原始数据
      val productType = value.productType
      val toursimType = value.toursimType
      val userID = value.userID
      val fee = value.fee

      //记录时间
      val curProcessTime = ctx.timerService().currentProcessingTime()
      val maxIntervalTimestamp :Long = Time.of(maxInterval,TimeUnit.MINUTES).toMilliseconds
      var nextProcessingTime = TimeWindow.getWindowStartWithOffset(curProcessTime, 0, maxIntervalTimestamp) + maxIntervalTimestamp

      //时间触发条件：当前处理时间到达或超过上次处理时间+间隔后触发本次窗口操作
      if(customerProcessState.value() == null){
        customerProcessState.update(new QProcessWindow(curProcessTime, curProcessTime))
        ctx.timerService().registerProcessingTimeTimer(nextProcessingTime)
      }
      val qProcessWindow :QProcessWindow = customerProcessState.value()
      if(curProcessTime >= qProcessWindow.end){
        qProcessWindow.start = qProcessWindow.end
        qProcessWindow.end = curProcessTime
      }
      customerProcessState.update(qProcessWindow)
      val startWindowTime = qProcessWindow.start
      val endWindowTime = qProcessWindow.end

      //PV等度量统计
      var ordersAcc :OrderAccData =  customerOrdersAccState.value
      if(null == ordersAcc){
        ordersAcc = new OrderAccData(QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO)
      }
      var totalOrders :Long = ordersAcc.orders + 1
      var totalFee :Long = ordersAcc.totalFee + fee
      customerOrdersAccState.update(new OrderAccData(totalOrders, totalFee))

      //UV判断
      var userKeys :mutable.Set[String] = customerUserState.value
      if(null == userKeys){
        userKeys = mutable.Set[String]()
      }
      if(!userKeys.contains(userID)){
        userKeys += userID
      }
      val users = userKeys.size
      customerUserState.update(userKeys)

      //数量触发条件：maxCount
      if(totalOrders >= maxCount){
        val orderDetailStatisData = OrderWideCustomerStatisData(productType, toursimType, startWindowTime, endWindowTime,totalOrders, users, totalFee)

        out.collect(orderDetailStatisData)
        customerOrdersAccState.clear()
        customerUserState.clear()
      }
    }


    /**
      * 定时器触发
      * @param timestamp
      * @param ctx
      * @param out
      */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[OrderWideAggDimData, OrderWideData, OrderWideCustomerStatisData]#OnTimerContext, out: Collector[OrderWideCustomerStatisData]): Unit = {
      val key :OrderWideAggDimData = ctx.getCurrentKey
      val productType = key.productType
      val toursimType = key.toursimType

      //时间触发条件：当前处理时间到达或超过上次处理时间+间隔后触发本次窗口操作
      if(customerProcessState.value() == null){
        customerProcessState.update(new QProcessWindow(timestamp, timestamp))
      }
      val maxIntervalTimestamp :Long = Time.of(maxInterval,timeUnit).toMilliseconds
      var nextProcessingTime = TimeWindow.getWindowStartWithOffset(timestamp, 0, maxIntervalTimestamp) + maxIntervalTimestamp
      ctx.timerService().registerProcessingTimeTimer(nextProcessingTime)
      val startWindowTime = customerProcessState.value().start

      //度量数据
      var ordersAcc :OrderAccData =  customerOrdersAccState.value
      if(null == ordersAcc){
        ordersAcc = new OrderAccData(QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO)
      }
      var totalOrders :Long = ordersAcc.orders + 1
      var totalFee :Long = ordersAcc.totalFee + QRealTimeConstant.COMMON_NUMBER_ZERO

      //UV判断
      var userKeys :mutable.Set[String] = customerUserState.value
      if(null == userKeys){
        userKeys = mutable.Set[String]()
      }
      val users = userKeys.size

      //触发
      val orderDetailStatisData = OrderWideCustomerStatisData(productType, toursimType, startWindowTime, timestamp,totalOrders, users, totalFee)
      out.collect(orderDetailStatisData)

      customerOrdersAccState.clear()
      customerUserState.clear()
    }
  }
```



### 第九节 CEP复杂事件处理

#### 背景说明

```
在实际工作中，可能会遇到要求数据能够实时的采集下来或者在高并发场景下业务逻辑以异步方式集合消息通道来进行数据处理，这样就涉及一个问题数据如何实时采集，虽然有像flume这样的采集框架，但它基于数据量大小和时间间隔的方式不一定能满足实时采集要求，所以消息通道+消息消费落地这种技术方案可以解决这类问题，数据通道可以采用kafka，消息消费可以采用Flink、SparkStreaming等框架完成。另外实时采集到的数据还可以校对实时计算结果或进行补救措施。
```



#### 用户浏览页面日志停留时长异常

##### 需求点

```
用户浏览页面日志停留时长普通情况下应该符合本行业的规律，远低于范围或高于范围的数据应该发出报警信息以预防并进一步分析原因，示例记录用户浏览页面场景下停留时长<5s或>200s的异常情况并进行追踪或报警处理
```



##### 技术分析

```
1 数据源
  基于用户浏览页面日志数据来完成
  
2 Flink CEP处理

3 数据输出
  * 消息队列kafka（保障线上报警实时性）
  * 分布式缓存 Redis（形成缓存级别报警库）
  * HDFS | S3 hdfs://hdfsCluster/data/xxx（用于追踪分析异常数据原因）
```



用户浏览页面日志停留时长异常处理过程(局部代码)

详细代码参考：com.qf.bigdata.realtime.flink.streaming.cep.UserLogsViewWarnHandler

```scala
/**
 * 4 设置复杂规则 cep
 *   规则：5分钟内连续连续 停留时长 小于X 大于Y 的情况出现3次以上
 */
val pattern :Pattern[UserLogPageViewData, UserLogPageViewData] =
Pattern.begin[UserLogPageViewData](QRealTimeConstant.FLINK_CEP_VIEW_BEGIN)
.where(//对应规则逻辑
    (value: UserLogPageViewData, ctx) => {
        val durationTime = value.duration.toLong
        durationTime < minDuration || durationTime > maxDuration
    }
)
.timesOrMore(times)//匹配规则次数
.consecutive() //连续匹配模式


/**
* 5 页面浏览告警数据流
*/
val viewPatternStream :PatternStream[UserLogPageViewData]= CEP.pattern(viewDStream, pattern.within(Time.minutes(timeRange)))
    
val viewDurationAlertDStream :DataStream[UserLogPageViewAlertData] = viewPatternStream.process(new UserLogsViewPatternProcessFun())
```



### 第十节 实时数据采集

#### 背景说明

```
在实际工作中，可能会遇到要求数据能够实时的采集下来或者在高并发场景下业务逻辑以异步方式集合消息通道来进行数据处理，这样就涉及一个问题数据如何实时采集，虽然有像flume这样的采集框架，但它基于数据量大小和时间间隔的方式不一定能满足实时采集要求，所以消息通道+消息消费落地这种技术方案可以解决这类问题，数据通道可以采用kafka，消息消费可以采用Flink、SparkStreaming等框架完成。另外实时采集到的数据还可以校对实时计算结果或进行补救措施。
```



#### 旅游产品订单数据实时采集

##### 需求点

```
旅游产品订单数据实时采集落地(HDFS)
```



##### 技术分析

```
1 数据源
  基于订单事实明细数据来完成
  
2 StreamingFileSink 流式文件数据输出源

3 数据输出文件系统(Flink支持的)
  * 本地文件 file:///data/xxxx
  * HDFS | S3 hdfs://hdfsCluster/data/xxx
```



实时数据采集落地过程(局部代码)

详细代码参考：com.qf.bigdata.realtime.flink.streaming.recdata.OrdersRecHandler

```scala
//数据实时采集落地
//旅游产品订单数据
val orderDetailDStream :DataStream[String] = ...

//4 数据实时采集落地
//数据落地路径
val outputPath :Path = new Path(output)
//落地大小阈值
val maxPartSize = 1024l *  maxSize
//落地时间间隔
val rolloverInl = TimeUnit.SECONDS.toMillis(rolloverInterval)
//无数据间隔时间
val inactivityInl = TimeUnit.SECONDS.toMillis(inactivityInterval)
//分桶检查点时间间隔
val bucketCheckInl = TimeUnit.SECONDS.toMillis(bucketCheckInterval)

//落地策略
val rollingPolicy :DefaultRollingPolicy[String,String] = DefaultRollingPolicy.create()
.withRolloverInterval(rolloverInl)
.withInactivityInterval(inactivityInl)
.withMaxPartSize(maxPartSize)
.build()

//数据分桶分配器
val bucketAssigner :BucketAssigner[String,String] = new DateTimeBucketAssigner(QRealTimeConstant.FORMATTER_YYYYMMDDHH)

//输出sink
val hdfsSink: StreamingFileSink[String] = StreamingFileSink
.forRowFormat(outputPath, new SimpleStringEncoder[String]("UTF-8"))
.withBucketAssigner(bucketAssigner)
.withRollingPolicy(rollingPolicy)
.withBucketCheckInterval(bucketCheckInl)
.build()

```























## 附录：

1 大数据全栈主要技术说明

```
一 数据采集
	1  埋点数据： 
		   由微服务后台以发送消息形式采集各种埋点数据
		   Flume日志采集
	2  业务数据： 
		   (1) 基于MYSQL的binlog日志
		   (2) 由微服务后台以消息形式发送
    3  外部接口数据：
    		由微服务后台进行相关处理保存在关系型数据库中或以消息形式发送(为了流量削峰)

二 数据通道
		考虑到高并发下的数据级量采用分布式消息队列 Kafka

三 数据计算
		离线： Spark
		实时： Flink
		交互式查询：
			(1) 实时明细 Druid
			(2) 实时聚合 Druid | Redis
			(3) 实时明细搜索 ES
			(4) 通用型 ClickHouse | Presto
			(5) 离线指标 Hive（数仓的集市数据或在此基础上的二次加工）
			(6) 离线或实时的多维分析结果 Kylin
			
四 任务调度
		Azkaban | airflow
		

五 数据存储
		离线： Hive | HBase | ES
		实时：	Druid | ES
		关联关系： Neo4j | JanusGraph
		
六 数据展示：
		离线展示： echars | apache superset
		实时展示： Grafana
```



2 旅游行业相关数据

```
1 携程数据分析
https://www.afenxi.com/24468.html

2 携程实时用户数据采集与分析
https://blog.csdn.net/imgxr/article/details/80129726

3 旅游研究院&携程大数据报告
http://www.ctaweb.org/html/2018-6/2018-6-29-9-4-22023.html
```


