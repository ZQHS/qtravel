package com.qf.bigdata.realtime.flink.streaming.rdo

/**
  * 维表数据DO(case calss)
  */
object QRealTimeDimDO {

  /**
    * 旅游产品维度数据
    */
  case class ProductDimDO(productID:String, productLevel:Int, productType:String,
                          depCode:String, desCode:String, toursimType:String)


}
