package com.qf.bigdata.realtime.util.DateUtil

import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date, GregorianCalendar}

import com.alibaba.fastjson.{JSON, JSONArray}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

/**
  * 时间工具
  */
case class ExposeBaseTime(ctime: String, hour: Int, weekday: Int, holiday: Boolean)

class DateUtil {}

object DateUtil {
  private val logger = LoggerFactory.getLogger(classOf[DateUtil])

  val PATTERN_YYYYMMDD =  DateTimeFormatter.ofPattern("yyyyMMdd")


  def dateFormat(time: Long, pattern: String): String = {
    if (time <= 0 || null == pattern) {
      return null
    }
    val datetime = LocalDateTime.ofInstant(new Date(time).toInstant, ZoneId.systemDefault())
    datetime.format(DateTimeFormatter.ofPattern(pattern))
  }

  def dateFormat(date: Date): String = {
    if (null == date) {
      return null
    }
    val datetime = LocalDateTime.ofInstant(date.toInstant, ZoneId.systemDefault())
    datetime.format(PATTERN_YYYYMMDD)
  }
}
