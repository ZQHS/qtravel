package com.qf.bigdata.realtime.flink.streaming.cep

import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDO.UserLogPageViewData
import org.apache.flink.cep.EventComparator

class UserLogsViewEventComparator extends EventComparator[UserLogPageViewData]{
  override def compare(o1: UserLogPageViewData, o2: UserLogPageViewData): Int = {
    val sid = o1.sid
    val sid2 = o2.sid
    sid.compareTo(sid2)
  }
}
