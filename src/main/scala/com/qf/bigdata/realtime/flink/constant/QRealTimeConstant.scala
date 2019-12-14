package com.qf.bigdata.realtime.flink.constant

/**
  * 实时场景使用常量
  */
object QRealTimeConstant {

  //kafka消费者配置文件
  val KAFKA_CONSUMER_CONFIG_URL = "kafka/flink/kafka-consumer.properties"
  //kafka生产者配置文件
  val KAFKA_PRODUCER_CONFIG_URL = "kafka/flink/kafka-producer.properties"

  //flink检查点间隔
  val FLINK_CHECKPOINT_INTERVAL :Long = 5000

  //本地模型下的默认并行度(cpu core)
  val DEF_LOCAL_PARALLELISM  = Runtime.getRuntime.availableProcessors

  //广播变量名称
  val BC_ACTIONS = "bc_actions"

  //外部传参名称
  val PARAMS_KEYS_APPNAME = "appname"
  val PARAMS_KEYS_TOPIC_FROM = "from_topic"
  val PARAMS_KEYS_TOPIC_TO = "to_topic"


  //kafka参数
  val TOPIC_FROM = "t_travel_ods"
  val TOPIC_TO = "t_travel_dw"
  val TOPIC_TO_WIDE = "t_travel_mid"

  //es索引
  val ES_INDEX_DW = "qf_travel_dw"
  val ES_INDEX_DM = "qf_travel_dm"


  //ES集群配置文件
  val ES_CONFIG_PATH = "es/es-config.json"


  //ES同记录写入并发重试次数
  val ES_RETRY_NUMBER = 15

  //ES索引中的时间列
  val esCt = "es_ct"
  val esUt = "es_ut"

  val ES_PV = "view_count"
  val ES_MAX = "max_price"
  val ES_MIN = "min_price"

  val KEY_ES_ID = "id"


  //=====基础信息==============================================================
  //kafka分区Key
  val KEY_KAFKA_ID = "KAFKA_ID"

  //请求ID
  val KEY_SID = "sid"
  //用户ID
  val KEY_USER_ID = "user_id"
  //用户设备号
  val KEY_USER_DEVICE = "user_device"
  //终端类型
  val KEY_CONSOLE_TYPE = "console_type"
  //用户设备类型
  val KEY_USER_DEVICE_TYPE = "user_device_type"
  //操作系统
  val KEY_OS = "os"
  //手机制造商
  val KEY_MANUFACTURER = "manufacturer"
  //电信运营商
  val KEY_CARRIER = "carrier"
  //网络类型
  val KEY_NETWORK_TYPE = "network_type"
  //用户所在地区
  val KEY_USER_REGION = "user_region"
  //用户所在地区IP
  val KEY_USER_REGION_IP = "user_region_ip"
  //经度
  val KEY_LONGITUDE = "lonitude"
  //纬度
  val KEY_LATITUDE = "latitude"


  //行为类型
  val KEY_ACTION = "action"

  //事件类型
  val KEY_EVENT_TYPE = "event_type"

  //事件目的
  val KEY_EVENT_ACTION = "event_action"


  //停留时长
  val KEY_DURATION = "duration"

  val KEY_DURATION_PAGE_NAGV_MIN = 3
  val KEY_DURATION_PAGE_NAGV_MAX = 90

  val KEY_DURATION_PAGE_VIEW_MIN = 3
  val KEY_DURATION_PAGE_VIEW_MAX = 60

  //创建时间
  val KEY_CT = "ct"
  //更新时间
  val KEY_UT = "ut"


  //=====扩展信息==============================================================

  val KEY_EXTS = "exts"

  //产品ID
  val KEY_PRODUCT_ID = "product_id"

  //产品列表
  val KEY_PRODUCT_IDS = "product_ids"

  //目标信息
  val KEY_EXTS_TARGET_ID = "target_id"
  val KEY_EXTS_TARGET_IDS = "target_ids"
  val KEY_EXTS_TARGET_KEYS = "target_keys"

  //=====查询信息==============================================================

  val KEY_EXTS_QUERY_TRAVEL_TIME = "q_travel_time" //行程天数

  val KEY_EXTS_QUERY_HOT_TARGET = "q_hot_target" //热门目的地

  val KEY_EXTS_QUERY_SEND = "q_send" //出发地

  val KEY_EXTS_QUERY_SEND_TIME = "q_send_time" //出发时间

  val KEY_EXTS_QUERY_PRODUCT_TYPE = "q_product_type" //产品类型：跟团、私家、半自助

  val KEY_EXTS_QUERY_PRODUCT_LEVEL = "q_product_level" //产品钻级


  //用户数量限制级别
  val USER_COUNT_LEVEL = 5


}
