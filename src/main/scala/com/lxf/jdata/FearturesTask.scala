package com.lxf.jdata


import com.lxf.jdata.DateUtil._
import com.lxf.jdata.LoggerUtil._
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.collection.mutable


object FearturesTask {

  case class Product(productId: Int, price: Double, cate: Int, para1: Double, para2: Double, para3: Double)

  case class UA(userId: Int, productId: Int, actionDate: String, actionNum: Int, actionType: Int, month: Int, day: Int)

  case class UA_p(userId: Int, productId: Int, actionDate: String, actionType_1_num: Int, actionType_2_num: Int, month: Int, day: Int)

  case class User(userID: Int, age: Int, sex: Int, userGrade: Int)

  case class Comment(userId: Int, commentTime: String, orderId: Int, scoreLevel: Int)

  case class Order(userId: Int, productId: Int, orderId: Int, orderDate: String, orderArea: Int, BuyNum: Int, month: Int, day: Int)

  case class UA_f(userId: Int, productId: Int, ua1_5: Int, ua2_5: Int, ua1_10: Int, ua2_10: Int, ua1_20: Int, ua2_20: Int, ua1_40: Int, ua2_40: Int, ua1_60: Int, ua2_60: Int, ua1_80: Int, ua2_80: Int, ua1_100: Int, ua2_100: Int, ua1_200: Int, ua2_200: Int, ua1_400: Int, ua2_400: Int)

  case class Order_f(userId: Int, productId: Int, o5: Int, o10: Int, o20: Int, o40: Int, o60: Int, o80: Int, o100: Int, o200: Int, o400: Int)

  case class Comment_f(userId: Int, productId: Int, s5: Int, s10: Int, s20: Int, s40: Int, s60: Int, s80: Int, s100: Int, s200: Int, s400: Int)

  val order_month = 4


  def main(args: Array[String]): Unit = {


    SetLogger
    val conf = new SparkConf().setMaster("local[4]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    import spark.sql
    val udf_sub = udf(sub _)
    val udf_sub2 = udf(sub2 _)

    /*用户表和产品表，只有基本特征，最后再整合*/
    val userDs = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_user_basic_info.csv")
      .map(x => User(x.getString(0).toInt, x.getString(1).toInt, x.getString(2).toInt, x.getString(3).toInt))
      .as[User]
    val user = userDs.map(x => {
      if (x.age < 0) {
        x.copy(age = 0)
      } else {
        x
      }
    })
    val user_t = new OneHotEncoder().setInputCol("age").setOutputCol("age_vec").transform(user)
    val user_tt = new OneHotEncoder().setInputCol("sex").setOutputCol("sex_vec").transform(user_t)
    val user_ttt = new OneHotEncoder().setInputCol("userGrade").setOutputCol("userGrade_vec").transform(user_tt)
    val user_p = user_ttt.drop("age", "sex", "userGrade")

    val productDs = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_sku_basic_info.csv")
      .map(x => Product(x.getString(0).toInt, x.getString(1).toDouble, x.getString(2).toInt, x.getString(3).toDouble, x.getString(4).toDouble, x.getString(5).toDouble))
      .as[Product]
    val product_t = productDs.map(x => {
      var m: Product = x.copy(cate = 0)
      x.cate match {
        case 101 => m = x.copy(cate = 0)
        case 1 => m = x.copy(cate = 1)
        case 83 => m = x.copy(cate = 2)
        case 71 => m = x.copy(cate = 3)
        case 30 => m = x.copy(cate = 4)
        case 46 => m = x.copy(cate = 5)
      }
      m
    })
    val product_tt = new OneHotEncoder().setInputCol("cate").setOutputCol("cate_vec").transform(product_t)
    val product_p = product_tt.drop("cate")


    /*处理行为表，评论表和订单表，均按天进行统计*/
    val ua_f = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_user_action.csv")
      .map(x => {
        val strings = x.getString(2).split("-")
        val month = strings(0) + strings(1)
        val day = strings(2)
        UA(x.getString(0).toInt, x.getString(1).toInt, x.getString(2), x.getString(3).toInt, x.getString(4).toInt, month.toInt, day.toInt)
      })
      .as[UA]
      .map(x =>
        UA_p(x.userId, x.productId, x.actionDate, if (x.actionType == 1) x.actionNum else 0, if (x.actionType == 2) x.actionNum else 0, x.month, x.day)
      )
      .as[UA_p]
      .withColumn("uaCountDay", udf_sub($"actionDate"))
      .filter($"month" < 201700 + order_month)
      .drop("actionDate", "month", "day")
      .groupBy("userId", "productId", "uaCountDay")
      .sum("actionType_1_num", "actionType_2_num")
      .rdd
      .map(
        x => ((x.getInt(0), x.getInt(1)), (x.getInt(2), Array(x.getLong(3), x.getLong(4))))
      )
      .groupByKey()
      .map {
        x => {
          import collection.mutable.Map
          var result: Map[Int, Array[Long]] = Map()
          for (elem <- x._2) {
            result += elem._1 -> elem._2
          }
          //行为数量按天统计  ua1表示浏览，ua2表示关注
          val (ua1_5, ua2_5) = getLastFullDayNumUA(result, 5)
          val (ua1_10, ua2_10) = getLastFullDayNumUA(result, 10)
          val (ua1_20, ua2_20) = getLastFullDayNumUA(result, 20)
          val (ua1_40, ua2_40) = getLastFullDayNumUA(result, 40)
          val (ua1_60, ua2_60) = getLastFullDayNumUA(result, 60)
          val (ua1_80, ua2_80) = getLastFullDayNumUA(result, 80)
          val (ua1_100, ua2_100) = getLastFullDayNumUA(result, 100)
          val (ua1_200, ua2_200) = getLastFullDayNumUA(result, 200)
          val (ua1_400, ua2_400) = getLastFullDayNumUA(result, 400)
          UA_f(x._1._1, x._1._2, ua1_5, ua2_5, ua1_10, ua2_10, ua1_20, ua2_20, ua1_40, ua2_40, ua1_60, ua2_60, ua1_80, ua2_80, ua1_100, ua2_100, ua1_200, ua2_200, ua1_400, ua2_400)
        }
      }.toDF()


    val orderDs = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_user_order.csv")
      .map(x => {
        val strings = x.getString(3).split("-")
        val month = strings(0) + strings(1)
        val day = strings(2)
        Order(x.getString(0).toInt, x.getString(1).toInt, x.getString(2).toInt, x.getString(3), x.getString(4).toInt, x.getString(5).toInt, month.toInt, day.toInt)
      })
      .as[Order].withColumn("orderDayCount", udf_sub($"orderDate"))
    orderDs.createTempView("morder")

    val comment_f = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_user_comment_score.csv")
      .map(x => Comment(x.getString(0).toInt, x.getString(1), x.getString(2).toInt, x.getString(3).toInt))
      .as[Comment]
      .withColumn("commentDayCount", udf_sub2($"commentTime"))
      .filter($"commentDayCount" > 0)
      .drop("commentTime")
      .join(orderDs.select("orderId", "productId").distinct(), Seq("orderId"))
      .drop("orderId")
      .rdd
      .map(x => ((x.getInt(0), x.getInt(3)), (x.getInt(2), x.getInt(1))))
      .groupByKey()
      .map {
        x => {
          import collection.mutable.Map
          var result: Map[Int, Int] = Map()
          for (elem <- x._2) {
            result += elem._1 -> elem._2
          }
          //评论评分按天统计
          val s5 = getLastFullDayScore(result, 5)
          val s10 = getLastFullDayScore(result, 10)
          val s20 = getLastFullDayScore(result, 20)
          val s40 = getLastFullDayScore(result, 40)
          val s60 = getLastFullDayScore(result, 60)
          val s80 = getLastFullDayScore(result, 80)
          val s100 = getLastFullDayScore(result, 100)
          val s200 = getLastFullDayScore(result, 200)
          val s400 = getLastFullDayScore(result, 400)

          Comment_f(x._1._1, x._1._2, s5, s10, s20, s40, s60, s80, s100, s200, s400)
        }

      }
      .toDF()


    //订单表处理
    val order_f = sql("select * from morder where month<20170" + order_month).drop("orderDate", "month", "day")
      .groupBy("userId", "productId", "orderDayCount")
      .agg(Map("buyNum" -> "sum", "orderArea" -> "min"))
      .rdd
      .map(x => (
        (x.getInt(0), x.getInt(1)), (x.getInt(2), Array(x.getLong(3), x.getInt(4)))))
      .groupByKey()
      .map {
        x => {
          import collection.mutable.Map
          var result: Map[Int, Array[Long]] = Map()
          for (elem <- x._2) {
            result += elem._1 -> elem._2
          }
          //购买数量按天统计
          val o5 = getLastFullDayNum(result, 5)
          val o10 = getLastFullDayNum(result, 10)
          val o20 = getLastFullDayNum(result, 20)
          val o40 = getLastFullDayNum(result, 40)
          val o60 = getLastFullDayNum(result, 60)
          val o80 = getLastFullDayNum(result, 80)
          val o100 = getLastFullDayNum(result, 100)
          val o200 = getLastFullDayNum(result, 200)
          val o400 = getLastFullDayNum(result, 400)
          Order_f(x._1._1, x._1._2, o5, o10, o20, o40, o60, o80, o100, o200, o400)
        }
      }
      .toDF()
    println("用户行为表")
    ua_f.show()
    println("用户评论表")
    comment_f.show()
    println("订单表")
    order_f.show()

    val features=order_f
      .join(ua_f, Seq("userId", "productId"), "left")
      .join(comment_f, Seq("userId", "productId"), "left")
      .join(user, Seq("userId"), "left")
      .join(product_t, Seq("productId"), "left")
      .na.fill(0)

    val label=orderDs
      .filter($"month" === (201700+order_month))
      .select("userId","productId","day")
      .withColumnRenamed("day","label")

    val train=features.join(label, Seq("userId", "productId"), "left").na.fill(0)

    train.write.mode("overwrite").parquet("outputfiles/train")
    spark.close()
  }



  def getLastFullDayNum(orderInfoByDay: mutable.Map[Int, Array[Long]], dayCount: Int) = {
    var buySum = 0
    for (d <- Range(0, dayCount)) {
      if (orderInfoByDay.contains(d)) {
        buySum += orderInfoByDay(d)(0).toInt
      }
    }
    buySum
  }

  def getLastFullDayNumUA(uaInfoByDay: mutable.Map[Int, Array[Long]], dayCount: Int) = {
    var (ua1, ua2) = (0, 0)
    for (d <- Range(0, dayCount)) {
      if (uaInfoByDay.contains(d)) {
        ua1 += uaInfoByDay(d)(0).toInt
        ua2 += uaInfoByDay(d)(1).toInt
      }
    }
    (ua1, ua2)
  }

  def getLastFullDayScore(commentInfoByDay: mutable.Map[Int, Int], dayCount: Int) = {
    var score = 0
    for (d <- Range(0, dayCount)) {
      if (commentInfoByDay.contains(d)) {
        commentInfoByDay(d) match {
          case 1 => score += 3
          case 2 => score += 1
          case 3 => score -= 4
        }
      }
    }
    score
  }
}
