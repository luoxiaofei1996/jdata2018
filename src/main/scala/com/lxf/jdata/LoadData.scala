package com.lxf.jdata

import ml.dmlc.xgboost4j.LabeledPoint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler}
import org.apache.spark.sql.SparkSession

object LoadData {

  case class Product(productId: Int, price: Double, cate: Int, para1: Double, para2: Double, para3: Double)

  case class UA(userId: Int, productId: Int, actionDate: String, actionNum: Int, actionType: Int,month:Int,day:Int)

  case class User(userID: Int, age: Int, sex: Int, userGrade: Int)

  case class Comment(userId: Int, commentTime: String, orderId: Int, scoreLevel: Int)

  case class Order(userId: Int, productId: Int, orderId: Int, orderDate: String, orderArea: Int, BuyNum: Int,month:Int,day:Int)

  def main(args: Array[String]): Unit = {

    SetLogger
    val conf = new SparkConf().setMaster("local[4]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    println("------------prodcut-------------")
    println("打印数据")
    val productDs = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_sku_basic_info.csv")
      .map(x => Product(x.getString(0).toInt, x.getString(1).toDouble, x.getString(2).toInt, x.getString(3).toDouble, x.getString(4).toDouble, x.getString(5).toDouble))
      .as[Product]
    productDs.show()
    println("统计种类")
    productDs
      .groupBy("cate")
      .count()
      .show()
    println("onehot准备,将cate重新编码")
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
    product_t.show()
    println("进行onehot编码")
    val product_tt = new OneHotEncoder().setInputCol("cate").setOutputCol("cate_vec").transform(product_t)
    product_tt.show()
    println("抽取特征")
    val product_p = product_tt.drop("cate")
    product_p.show()


    println("------------UA-------------")
    println("打印数据")
    val UADs = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_user_action.csv")
      .map(x =>{
        val strings = x.getString(2).split("-")
        val month=strings(0)+strings(1)
        val day=strings(2)
        UA(x.getString(0).toInt, x.getString(1).toInt, x.getString(2), x.getString(3).toInt, x.getString(4).toInt,month.toInt,day.toInt)})
      .as[UA]
    UADs.show()
    println("统计种类")
    UADs.groupBy("actionType").count().show()
    println("统计月份")
    UADs.groupBy("month").count().show()
    println("对actionType进行onehot编码")
    val UA_t = new OneHotEncoder().setInputCol("actionType").setOutputCol("actionType_vec").transform(UADs)
    UA_t.show()
    println("抽取特征")
    val UA_p=UA_t.drop("actionType")
    UA_p.show()

    println("------------user-------------")
    println("打印数据")
    val userDs = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_user_basic_info.csv")
      .map(x => User(x.getString(0).toInt, x.getString(1).toInt, x.getString(2).toInt, x.getString(3).toInt))
      .as[User]
    userDs.show()
    println("统计年龄")
    userDs.groupBy("age").count().show()
    println("统计性别")
    userDs.groupBy("sex").count().show()
    println("统计等级")
    userDs.groupBy("userGrade").count().show()
    println("总数")
    println(userDs.count())

    println("去除负值")
    val user=userDs.map(x=> {
      if(x.age < 0){
        x.copy(age = 0)
      }else{
        x
      }
    })
    user.show()

    println("进行onehot编码")
    val user_t = new OneHotEncoder().setInputCol("age").setOutputCol("age_vec").transform(user)
    val user_tt = new OneHotEncoder().setInputCol("sex").setOutputCol("sex_vec").transform(user_t)
    val user_ttt = new OneHotEncoder().setInputCol("userGrade").setOutputCol("userGrade_vec").transform(user_tt)
    user_ttt.show()
    println("抽取特征")
    val user_p=user_ttt.drop("age","sex","userGrade")
    user_p.show()

    println("------------comment-------------")
    println("打印数据")
    val commentDs = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_user_comment_score.csv")
      .map(x => Comment(x.getString(0).toInt, x.getString(1), x.getString(2).toInt, x.getString(3).toInt))
      .as[Comment]
    commentDs.show()
    println("统计评分等级")
    commentDs.groupBy("scoreLevel").count().show()
    println("进行onehot编码")
    val comment_t = new OneHotEncoder().setInputCol("scoreLevel").setOutputCol("scoreLevel_vec").transform(commentDs)
    comment_t.show()
    println("抽取特征")
    val comment_p=comment_t.drop("scoreLevel")
    comment_p.show()

    println("------------order-------------")
    println("打印数据")
    val orderDs = spark.read.option("header", "true").option("sep", ",").format("csv").load("jdata2018/jdata_user_order.csv")
      .map(x => {
        val strings = x.getString(3).split("-")
        val month = strings(0) + strings(1)
        val day = strings(2)
        Order(x.getString(0).toInt, x.getString(1).toInt, x.getString(2).toInt, x.getString(3), x.getString(4).toInt, x.getString(5).toInt, month.toInt, day.toInt)
      })
      .as[Order]
    orderDs.show()
    println("统计月份")
    orderDs.groupBy("month").count().show()
    println("统计4月下单人数")
    println(orderDs.where("month = 201704").groupBy("userId").count().count())
    println("统计下单区域")
    orderDs.groupBy("orderArea").count().show()
    println("有多少个下单区域")
    println(orderDs.groupBy("orderArea").count().count())

    println("对orderArea进行onehot编码")
    val order_t = new OneHotEncoder().setInputCol("orderArea").setOutputCol("orderArea_vec").transform(orderDs)
    order_t.show()
    println("特征抽取")
    val order_p=order_t.drop("scoreLevel")
    order_p.show()

    spark.close()
  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }
}
