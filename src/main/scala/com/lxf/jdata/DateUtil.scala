package com.lxf.jdata

import java.text.SimpleDateFormat

import com.lxf.jdata.FearturesTask.order_month

object DateUtil {
  //计算日期差
  def sub( date: String): Int = {
    if ( date == null) {
      100000
    } else {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val time1 = sdf.parse(s"""2017-0$order_month-01""").getTime
      val time2 = sdf.parse(date).getTime
      val x = (time1 - time2) / (1000 * 3600 * 24)
      x.toInt
    }
  }


  def sub2( date: String): Int = {
    if ( date == null) {
      100000
    } else {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val sdf2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val time1 = sdf.parse(s"""2017-0$order_month-01""").getTime
      val time2 = sdf2.parse(date).getTime
      val x = (time1 - time2) / (1000 * 3600 * 24)
      x.toInt
    }
  }
}
