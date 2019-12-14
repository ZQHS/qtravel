package com.qf.bigdata.realtime.flink.functions

/**
  * 实时系统中涉及的各种DO(case calss)
  */
object QRealTimeDO {

  /**
    * 用户行为日志维表数据
    */
  case class ActionDim(code:String, desc:String, remark:String)



  /**
    * 用户行为日志原始数据
    */
  case class UserLogODSDo(sid:String, device:String, deviceType:String, os:String,
                          userID:String,userRegion:String, userIP:String, lon:String, lat:String,
                          manufacturer:String, carrier:String, networkType:String, exts:String,
                          action:String, eventType:String, ct:String)

  /**
    * 用户行为日志明细数据
    */
  case class UserLogDWDo(sid:String, device:String, deviceType:String, os:String,
                         userID:String,userRegion:String, userIP:String, lon:String, lat:String,
                         manufacturer:String, carrier:String, networkType:String, duration:String,
                         action:String, eventType:String, ct:String, targetID:String,
                         travelTime:String, productType:String, hotTarget:String, travelSend:String,
                         travelSendTime:String, productLevel:String)


  /**
    * 用户行为日志明细数据
    * aer_id => action_eventtype_userregion
    */
  case class UserLogAggDo(id :String , sid:String, duration:String, ct:String)


//  {
//    "os": "2",
//    "user_device_type": "9",
//    "lonitude": "115.84383",
//    "q_travel_time": "1",
//    "q_product_type": "02",
//    "latitude": "25.938",
//    "user_region_ip": "178.7.230.213",
//    "user_region": "360781",
//    "target_id": "P72",
//    "user_device": "68237",
//    "sid": "20191212160000xffki",
//    "manufacturer": "04",
//    "duration": "6",
//    "q_hot_target": "152921",
//    "ct": "1576147860000",
//    "carrier": "3",
//    "q_send": "360781",
//    "event_type": "04",
//    "KAFKA_ID": "i553k656gk",
//    "user_id": "88376",
//    "action": "05",
//    "network_type": "2",
//    "q_send_time": "202001",
//    "q_product_level": "4"
//  }


}
