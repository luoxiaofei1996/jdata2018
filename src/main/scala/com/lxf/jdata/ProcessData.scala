package com.lxf.jdata

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf


object ProcessData {

  case class Product(productId: Int, price: Double, cate: Int, para1: Double, para2: Double, para3: Double)

  case class UA(userId: Int, productId: Int, actionDate: String, actionNum: Int, actionType: Int, month: Int, day: Int)

  case class UA_p(userId: Int, productId: Int, actionDate: String, actionType_1_num: Int, actionType_2_num: Int, month: Int, day: Int)

  case class User(userID: Int, age: Int, sex: Int, userGrade: Int)

  case class Comment(userId: Int, commentTime: String, orderId: Int, scoreLevel: Int)

  case class Order(userId: Int, productId: Int, orderId: Int, orderDate: String, orderArea: Int, BuyNum: Int, month: Int, day: Int)
  val order_month = 4

  def main(args: Array[String]): Unit = {




    val conf = new SparkConf().setMaster("local[4]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    import spark.sql

    val productDs = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_sku_basic_info.csv")
      .map(x => Product(x.getString(0).toInt, x.getString(1).toDouble, x.getString(2).toInt, x.getString(3).toDouble, x.getString(4).toDouble, x.getString(5).toDouble))
      .as[Product]
    productDs.show()
    productDs.groupByKey(x=>x.cate).mapGroups((x,array)=>{
      val pro=array.toArray[Product]
      pro(0)
    }).show()

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


    val UADs = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_user_action.csv")
      .map(x => {
        val strings = x.getString(2).split("-")
        val month = strings(0) + strings(1)
        val day = strings(2)
        UA(x.getString(0).toInt, x.getString(1).toInt, x.getString(2), x.getString(3).toInt, x.getString(4).toInt, month.toInt, day.toInt)
      })
      .as[UA]
      .map(x =>
        UA_p(x.userId, x.productId, x.actionDate, if(x.actionType == 1) x.actionNum else 0, if(x.actionType == 2) x.actionNum else 0, x.month, x.day)
      )
      .as[UA_p]
      .rdd
      .map(x=>((x.userId,x.productId),(Array(x.actionType_1_num,x.actionType_2_num),x.actionDate)))
      .groupByKey()
      .map{
        x=> {
          var map=Map[Int,Array[Int]]()
          for (elem <- x._2) {
            map += (sub(elem._2) -> elem._1)
          }
          (x._1,map)
        }

      }
      .take(10).foreach(x=> {
      print(x._1 +":")
      var result =Array
      x._2.foreach(x=>{
        print(x._1+"->")
        print(x._2.mkString("[",",","]"))
      })
      println()
    })




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

    val commentDs = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_user_comment_score.csv")
      .map(x => Comment(x.getString(0).toInt, x.getString(1), x.getString(2).toInt, x.getString(3).toInt))
      .as[Comment]
    val comment_t = new OneHotEncoder().setInputCol("scoreLevel").setOutputCol("scoreLevel_vec").transform(commentDs)
    val comment_p = comment_t.drop("scoreLevel")

    val orderDs = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_user_order.csv")
      .map(x => {
        val strings = x.getString(3).split("-")
        val month = strings(0) + strings(1)
        val day = strings(2)
        Order(x.getString(0).toInt, x.getString(1).toInt, x.getString(2).toInt, x.getString(3), x.getString(4).toInt, x.getString(5).toInt, month.toInt, day.toInt)
      })
      .as[Order]
    val order_t = new OneHotEncoder().setInputCol("orderArea").setOutputCol("orderArea_vec").transform(orderDs)
    val order_p = order_t.drop("orderArea")
    order_p.createTempView("morder")
    println("过滤出目标购买信息")
    val order_l = sql("select * from morder where month<20170" + order_month)


    sql(s"""select userId,productId,min(day) day from morder where month=20170$order_month group by userId,productId""").createTempView("temp")

    val udf_sub = udf(sub _)
    val train = sql("select o.* from temp t left join morder o on t.userId=o.userId and t.productId=o.productId and t.day=o.day")
      .withColumnRenamed("orderDate", "labelDate")
      .withColumnRenamed("orderId", "labelId")
      .join(product_p, "productId")
      .join(user_p, "userId")
      .join(order_l.drop("month", "day"), Seq("userId", "productId"), "left")
      .withColumn("daysFromBuy", udf_sub( $"orderDate"))
      .join(comment_p, Seq("orderId"), "left")
    //      .join(UA_p.drop("month","day"),Seq("userId", "productId"),"left")
    //      .show()
    spark.sparkContext.parallelize(Array(1,2,3),4)
    spark.sparkContext.makeRDD(Array(1.2,3),5)
    order_p.join(comment_p,Seq("orderId"),"left").show()

    spark.close()
  }

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

}
