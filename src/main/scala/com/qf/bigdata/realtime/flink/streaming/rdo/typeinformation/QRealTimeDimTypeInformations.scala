package com.qf.bigdata.realtime.flink.streaming.rdo.typeinformation

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}

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
