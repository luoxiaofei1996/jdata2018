package com.lxf.salestrain

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object StatisticUtil {
  /**
    * 输入按时间聚合过的dataframe，输出统计后的dataframe
    * 输出：前1个单位，前2个单位，前3个单位，。。。前10个单位
    *
    * @param spark
    * @param df            需要统计的dataframe
    * @param cols           需要统计的字段
    * @param timeCol       时间字段
    * @param idCol         id字段
    * @param targetTime    要预测的时间点
    * @param statisticTime 统计的时间区间
    * @return
    */
  def statisticDf(
                   spark: SparkSession,
                   df: DataFrame,
                   cols: Array[String],
                   timeCol: String,
                   idCol: String,
                   targetTime: Int,
                   statisticTime: Int = 30) = {
    import spark.implicits._
    val colsSize = cols.length
    df
      .rdd
      .map(x => {
        val array = new ArrayBuffer[Double]
        cols.foreach(col => array.append(x.get(x.fieldIndex(col)).toString.toDouble))
        val key = x.get(x.fieldIndex(idCol)).toString
        val value = Map(x.get(x.fieldIndex(timeCol)).toString.toInt -> array.toArray)
        (key, value)
      })
      .combineByKey(
        (v) => v,
        (acc: Map[Int, Array[Double]], v: Map[Int, Array[Double]]) => acc ++ v,
        (acc1: Map[Int, Array[Double]], acc2: Map[Int, Array[Double]]) => acc1 ++ acc2
      )
      .map(x => {
        //id
        val id = x._1
        //某个id的统计值
        val map = x._2
        //将所有的特征保存在array中
        var features = ArrayBuffer[Double]()
        //计算前elem个单位的统计量和
        var sum = new Array[Double](colsSize)
        for (elem <- 0 to 30) {
          val feature = map.get(targetTime - elem).getOrElse(new Array[Double](colsSize))
          for (i <- Range(0, colsSize)) {
            sum(i) += feature(i)
          }
          features.appendAll(sum)
        }
        (id, Vectors.dense(features.toArray))
      })
      .toDF("id", "features")
  }

}
