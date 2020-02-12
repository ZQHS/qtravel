package com.qf.bigdata.realtime.flink.streaming.rdo

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
  case class UserLogData(sid:String, userDevice:String, userDeviceType:String, os:String,
                          userID:String,userRegion:String, userRegionIP:String, lonitude:String, latitude:String,
                          manufacturer:String, carrier:String, networkType:String, duration:String, exts:String,
                          action:String, eventType:String, ct:Long)





  /**
    * 用户行为启动日志数据
    */
  case class UserLogLaunchData(userDevice:String, userID:String,userRegion:String, userRegionIP:String, networkType:String, ct:Long)


  /**
    * 启动日志统计数据
    */
  case class UserLogAggDimMeanData(access:Long, users:Long, ct:Long)


  /**
    * 用户行为日志度量数据
    */
  case class UserLogAggMeanData(access:Long, users:Long)






  /**
    *用户行为日志页面浏览操作数据
    */
  case class UserLogPageViewData(sid:String, userDevice:String, userDeviceType:String, os:String,
                              userID:String,userRegion:String, userRegionIP:String, lonitude:String, latitude:String,
                              manufacturer:String, carrier:String, networkType:String, duration:String,
                              action:String, eventType:String, ct:Long, targetID:String)


  /**
    *用户行为日志页面浏览操作数据(告警)
    */
  case class UserLogPageViewAlertData(userDevice:String, userID:String,userRegion:String,
                                      userRegionIP:String, duration:String, ct:Long)



  /**
    * 页面浏览日志统计数据
    */
  case class UserLogPageViewAggMeanData(access:Long, users:Long, totalDuration:Long, avgDuration:Long, maxTimestamp:Long)

  /**
    * 页面浏览日志统计数据(停留时长不符合需求)
    */
  case class UserLogPageViewLowDurationAggMeanData(access:Long, users:Long, totalDuration:Long,maxTimestamp:Long)



  /**
    *用户行为日志点击操作数据
    */
  case class UserLogClickData(sid:String, userDevice:String, userDeviceType:String, os:String,
                         userID:String,userRegion:String, userRegionIP:String, lonitude:String, latitude:String,
                         manufacturer:String, carrier:String, networkType:String,
                         action:String, eventType:String, ct:Long, targetID:String, eventTargetType:String)

  /**
    *用户行为日志点击行为维度数据
    */
  case class UserLogClickDimData(userRegion:String, eventTargetType:String)

  /**
    *用户行为日志点击行为度量数据
    */
  case class UserLogClickMeanData(access:Long, users:Long)

  /**
    *用户行为日志点击行为统计输出数据
    */
  case class UserLogClickDimMeanData(userRegion:String, eventTargetType:String, access:Long, users:Long, startWindowTime:Long, endWindowTime:Long)


  /**
    * 用户行为日志产品列表浏览数据
    */
  case class UserLogViewListData(sid:String, userDevice:String, userDeviceType:String, os:String,
                                 userID:String,userRegion:String, userRegionIP:String, lonitude:String, latitude:String,
                                 manufacturer:String, carrier:String, networkType:String, duration:String, exts:String,
                                 action:String, eventType:String, ct:Long, hotTarget:String)


  /**
    *用户行为日志产品列表浏览数据
    */
  case class UserLogViewListFactData(sid:String, userDevice:String, userDeviceType:String, os:String,
                              userID:String,userRegion:String, userRegionIP:String, lonitude:String, latitude:String,
                              manufacturer:String, carrier:String, networkType:String, duration:String,
                              action:String, eventType:String, ct:Long,
                              targetID:String, hotTarget:String, travelSend:String, travelSendTime:String,
                              travelTime:String, productLevel:String, productType:String)


  //===订单业务=================================================================================

  /**
    *订单明细数据
    */
  case class OrderDetailData(orderID:String, userID:String, productID:String, pubID:String,
                             userMobile:String, userRegion:String, traffic:String, trafficGrade:String, trafficType:String,
                             price:Int, fee:Int, hasActivity:String,
                             adult:String, yonger:String, baby:String, ct:Long)

  case class OrderDetailSimData(orderID:String, userID:String, productID:String, pubID:String,
                             userRegion:String, traffic:String,
                             price:Int, fee:Int, hasActivity:String,ct:Long)


  /**
    * 订单会话窗口维度列集
    */
  case class OrderDetailSessionDimData(traffic:String, hourTime:String)


  case class OrderTrafficDimData(productID:String, traffic:String)

  case class OrderTrafficDimMeaData(productID:String, traffic:String, startWindowTime:Long, endWindowTime:Long,orders:Long, totalFee:Long)

  case class OrderTrafficMidData(productID:String, traffic:String, orders:Long, members:Long,totalFee:Long)

  /**
    * 订单宽表会话时间窗口统计结果列集
    */
  case class OrderDetailSessionDimMeaData(traffic:String, startWindowTime:Long, endWindowTime:Long,orders:Long, maxFee:Long, totalFee:Long, members:Long, avgFee:Double)



  /**
    * 处理时间窗口类
    * @param start 开始时间
    * @param end 结束时间
    */
  case class QProcessWindow(var start:Long, var end:Long)



  /**
    * 订单统计维度列集
    */
  case class OrderDetailAggDimData(userRegion:String, traffic:String)


  /**
    * 订单统计累加器
    */
  case class OrderAccData(orders:Long, totalFee:Long)


  /**
    * 订单宽表时间窗口统计度量列集
    */
  case class OrderDetailTimeAggMeaData(orders:Long, maxFee:Long, totalFee:Long, members:Long)



  /**
    * 订单宽表时间窗口统计结果列集
    */
  case class OrderDetailTimeAggDimMeaData(userRegion:String, traffic:String, startWindowTime:Long, endWindowTime:Long,orders:Long, maxFee:Long, totalFee:Long, members:Long, avgFee:Double)


  /**
    * 订单时间窗口统计结果列集
    */
  case class OrderDetailStatisData(traffic:String, etTime:String,orders:Long, users:Long, totalFee:Long, ptTime:String)


  case class OrderDetailStatisCustomerData(traffic:String, etTime:String, ptTime:String, orders:Long, users:Long, totalFee:Long)


  case class OrderDetailTimeStatisData(userRegion:String, traffic:String, startWindowTime:Long, endWindowTime:Long,orders:Long, users:Long, totalFee:Long)


  case class OrderWindowStatisData(traffic:String, etTime:String, startWindowTime:Long, endWindowTime:Long,orders:Long, totalFee:Long)


  //---订单宽表-----------------------------------------


  /**
    *订单明细数据(宽表数据)
    */
  case class OrderWideData(orderID:String, userID:String, productID:String, pubID:String,
                           userMobile:String, userRegion:String, traffic:String, trafficGrade:String, trafficType:String,
                           price:Int, fee:Long, hasActivity:String,
                           adult:String, yonger:String, baby:String, ct:Long,
                           productLevel:Int, productType:String, toursimType:String, depCode:String, desCode:String)


  /**
    *订单明细数据(多维表宽表数据)
    */
  case class OrderMWideData(orderID:String, userID:String, productID:String, pubID:String,
                           userMobile:String, userRegion:String, traffic:String, trafficGrade:String, trafficType:String,
                           price:Int, fee:Long, hasActivity:String,
                           adult:String, yonger:String, baby:String, ct:Long,
                           productLevel:Int, productType:String, toursimType:String, depCode:String, desCode:String,
                           pubStar:String, pubGrade:String, isNational:String)



  /**
    * 订单宽表统计维度列集
    */
  case class OrderWideAggDimData(productType:String, toursimType:String)

  /**
    * 订单宽表计数窗口统计度量列集
    */
  case class OrderWideCountAggMeaData(orders:Long, maxFee:Long, totalFee:Long, members:Long)

  /**
    * 订单宽表计数窗口统计结果列集
    */
  case class OrderWideCountAggDimMeaData(productType:String, toursimType:String,endWindowTime:Long, orders:Long, maxFee:Long, totalFee:Long, members:Long, avgFee:Double)


  /**
    * 订单宽表时间窗口统计度量列集
    */
  case class OrderWideTimeAggMeaData(orders:Long, maxFee:Long, totalFee:Long, members:Long)



  /**
    * 订单宽表时间窗口统计结果列集
    */
  case class OrderWideTimeAggDimMeaData(productType:String, toursimType:String, startWindowTime:Long, endWindowTime:Long,orders:Long, maxFee:Long, totalFee:Long, members:Long, avgFee:Double)


  /**
    * 订单宽表统计结果列集合
    */
  case class OrderWideCustomerStatisData(productType:String, toursimType:String, startWindowTime:Long, endWindowTime:Long,orders:Long, users:Long, totalFee:Long)


//===============================================================================

}
