package com.qf.bigdata.realtime.flink.streaming.funs.orders


import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO._
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDimDO.ProductDimDO
import com.qf.bigdata.realtime.util.json.JsonUtil
import org.apache.flink.api.common.functions.{MapFunction}
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector


/**
  * 旅游订单业务相互的自定义函数
  */
object OrdersETLFun {


  /**
    * 订单明细数据(用于开窗处理)转换函数
    */
  class OrderDetailDataMapFun extends MapFunction[String,OrderDetailData]{

    override def map(value: String): OrderDetailData = {

      //根据行为和事件数据进行扩展信息提取
      val record :java.util.Map[String,String] = JsonUtil.json2object(value, classOf[java.util.Map[String,String]])

      //订单ID
      val orderID :String = record.getOrDefault(QRealTimeConstant.KEY_ORDER_ID,"")

      //用户ID
      val userID :String = record.getOrDefault(QRealTimeConstant.KEY_USER_ID,"")

      //产品ID
      val productID :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_ID,"")

      //酒店ID
      val pubID :String = record.getOrDefault(QRealTimeConstant.KEY_PUB_ID,"")

      //用户手机
      val userMobile :String = record.getOrDefault(QRealTimeConstant.KEY_USER_MOBILE,"")

      //用户所在地区
      val userRegion :String = record.getOrDefault(QRealTimeConstant.KEY_USER_REGION,"")

      //交通出行
      val traffic :String = record.getOrDefault(QRealTimeConstant.KEY_TRAFFIC,"")

      //交通出行等级
      val trafficGrade :String = record.getOrDefault(QRealTimeConstant.KEY_TRAFFIC_GRADE,"")

      //交通出行类型
      val trafficType :String = record.getOrDefault(QRealTimeConstant.KEY_TRAFFIC_TYPE,"")

      //交通出行类型
      val price :Int = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_PRICE,"").toInt

      //交通出行类型
      val fee :Int = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_FEE,"").toInt


      //折扣
      val hasActivity :String = record.getOrDefault(QRealTimeConstant.KEY_HAS_ACTIVITY,"")

      //成人、儿童、婴儿
      val adult :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_ADULT,"")
      val yonger :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_YONGER,"")
      val baby :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_BABY,"")

      //事件时间
      val ct :Long = record.getOrDefault(QRealTimeConstant.KEY_ORDER_CT,"").toLong

      OrderDetailData(orderID, userID, productID, pubID,
        userMobile, userRegion, traffic, trafficGrade, trafficType,
        price, fee, hasActivity:String,
        adult, yonger, baby, ct)
    }
  }

  /**
    * 订单宽表数据
    */
  class OrderWideDataMapFun extends MapFunction[String,OrderWideData]{

    override def map(value: String): OrderWideData = {

      //根据行为和事件数据进行扩展信息提取
      val record :java.util.Map[String,String] = JsonUtil.json2object(value, classOf[java.util.Map[String,String]])

      //订单ID
      val orderID :String = record.getOrDefault(QRealTimeConstant.KEY_ORDER_ID,"")

      //用户ID
      val userID :String = record.getOrDefault(QRealTimeConstant.KEY_USER_ID,"")

      //产品ID
      val productID :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_ID,"")

      //酒店ID
      val pubID :String = record.getOrDefault(QRealTimeConstant.KEY_PUB_ID,"")

      //用户手机
      val userMobile :String = record.getOrDefault(QRealTimeConstant.KEY_USER_MOBILE,"")

      //用户所在地区
      val userRegion :String = record.getOrDefault(QRealTimeConstant.KEY_USER_REGION,"")

      //交通出行
      val traffic :String = record.getOrDefault(QRealTimeConstant.KEY_TRAFFIC,"")

      //交通出行等级
      val trafficGrade :String = record.getOrDefault(QRealTimeConstant.KEY_TRAFFIC_GRADE,"")

      //交通出行类型
      val trafficType :String = record.getOrDefault(QRealTimeConstant.KEY_TRAFFIC_TYPE,"")

      //交通出行类型
      val price :Int = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_PRICE,"").toInt

      //交通出行类型
      val fee :Int = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_FEE,"").toInt


      //折扣
      val hasActivity :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_FEE,"")

      //成人、儿童、婴儿
      val adult :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_ADULT,"")
      val yonger :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_YONGER,"")
      val baby :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_BABY,"")

      //产品维度数据相关
      val productLevel:Int = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_LEVEL, "").toInt
      val productType:String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_TYPE, "")
      val toursimType:String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_TOURSIMTYPE, "")
      val depCode:String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_DEPCODE, "")
      val desCode:String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_DESCODE, "")


      //事件时间
      val ct :Long = record.getOrDefault(QRealTimeConstant.KEY_ORDER_CT,"").toLong

      OrderWideData(orderID:String, userID:String, productID:String, pubID:String,
        userMobile:String, userRegion:String, traffic:String, trafficGrade:String, trafficType:String,
        price:Int, fee:Long, hasActivity:String,
        adult:String, yonger:String, baby:String, ct:Long,
        productLevel:Int, productType:String, toursimType:String, depCode:String, desCode:String)
    }
  }



  /**
    * 订单数据广播处理
    * 订单开窗宽表数据
    */
  class OrderWideBCFunction(bcName:String) extends BroadcastProcessFunction[OrderDetailData, ProductDimDO, OrderWideData]{

    val productMSDesc = new MapStateDescriptor[String,ProductDimDO](bcName, createTypeInformation[String], createTypeInformation[ProductDimDO])

    //维度数据收集器
    var products :Seq[ProductDimDO] = List[ProductDimDO]()


    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
    }

    //流式数据处理
    override def processElement(value: OrderDetailData, ctx: BroadcastProcessFunction[OrderDetailData, ProductDimDO, OrderWideData]#ReadOnlyContext, out: Collector[OrderWideData]): Unit = {

      val productBState :ReadOnlyBroadcastState[String,ProductDimDO] = ctx.getBroadcastState(productMSDesc);

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
        //println(s"""OrderWideBCFunction.productid[${orderProductID}] not match !""")
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


    //广播数据处理
    override def processBroadcastElement(value: ProductDimDO, ctx: BroadcastProcessFunction[OrderDetailData, ProductDimDO, OrderWideData]#Context, out: Collector[OrderWideData]): Unit = {
      val productBState :BroadcastState[String, ProductDimDO] = ctx.getBroadcastState(productMSDesc);
      products = products.:+(value)

      val key = value.productID
      productBState.put(key, value);
    }
  }




  /**
    * 订单数据广播处理
    * 订单开窗宽表数据
    */
  class OrderWideAsyncBCFunction(bcName:String) extends BroadcastProcessFunction[OrderDetailData, Row, OrderWideData]{

    val productMSDesc = new MapStateDescriptor[String,Row](bcName, createTypeInformation[String], createTypeInformation[Row])

    //维度数据收集器
    var products :Seq[Row] = List[Row]()


    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
    }

    //流式数据处理
    override def processElement(value: OrderDetailData, ctx: BroadcastProcessFunction[OrderDetailData, Row, OrderWideData]#ReadOnlyContext, out: Collector[OrderWideData]): Unit = {

      val productBState :ReadOnlyBroadcastState[String,Row] = ctx.getBroadcastState(productMSDesc);

      val orderProductID :String = value.productID
      if(productBState.contains(orderProductID)){
        val row :Row = productBState.get(orderProductID)

        val productID = row.getField(0).toString
        val productLevel = row.getField(0).toString.toInt
        val productType = row.getField(0).toString
        val depCode = row.getField(0).toString
        val desCode = row.getField(0).toString
        val toursimType = row.getField(0).toString

        val orderWide = OrderWideData(value.orderID, value.userID, value.productID, value.pubID,
          value.userMobile, value.userRegion, value.traffic, value.trafficGrade, value.trafficType,
          value.price, value.fee, value.hasActivity,
          value.adult, value.yonger, value.baby, value.ct,
          productLevel, productType, toursimType, depCode, desCode)

        println(s"""orderWide=${JsonUtil.gObject2Json(orderWide)}""")
        out.collect(orderWide)

      }else{
        println(s"""OrderWideBCFunction.productid[${orderProductID}] not match !""")
        val notMatch = "-1"

        val orderWide = OrderWideData(value.orderID, value.userID, value.productID, value.pubID,
          value.userMobile, value.userRegion, value.traffic, value.trafficGrade, value.trafficType,
          value.price, value.fee, value.hasActivity,
          value.adult, value.yonger, value.baby, value.ct,
          notMatch.toInt, notMatch, notMatch, notMatch, notMatch)
        println(s"""orderWide=${JsonUtil.gObject2Json(orderWide)}""")
        out.collect(orderWide)
      }
    }


    //广播数据处理
    override def processBroadcastElement(value: Row, ctx: BroadcastProcessFunction[OrderDetailData, Row, OrderWideData]#Context, out: Collector[OrderWideData]): Unit = {
      val productBState :BroadcastState[String, Row] = ctx.getBroadcastState(productMSDesc);
      products = products.:+(value)

      val key = value.getField(0).toString
      productBState.put(key, value);
    }
  }





}
