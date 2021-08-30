package com.qianfeng.bigdata.realtime.flink.streaming.rdo.typeinformation

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}

/**
 * 产品维度对应的类型
 */
object QRealTimeDimTypeInformation {

  /**
   * 产品维度表结构
   * @return
   */
  def getProductDimFieldTypeInfos():List[TypeInformation[_]] ={
    //先定义一个变量
    var colTypeInfos :List[TypeInformation[_]] = List[TypeInformation[_]]()
    //向集合中添加类型
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.INT_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
   //返回类型的集合
    colTypeInfos
  }
}
