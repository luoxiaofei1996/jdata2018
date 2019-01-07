package com.lxf.salestrain

import com.lxf.salestrain.LoggerUtil._
import com.lxf.salestrain.StatisticUtil._
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object TrainModelViaWeekStatistic {



  def extractLabel(df: DataFrame, idCol: String, labelCol: String, filter: String): DataFrame = {
    df.filter(filter).select(idCol, labelCol)
  }


  def main(args: Array[String]): Unit = {
    SetLogger
    val conf = new SparkConf().setMaster("local[9]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    var train = spark.read.parquet("output/train2")
    train = train.withColumn("weekNum2", -col("weekNum") + 61).drop("weekNum").drop("goods_id")
    train = train.cache()
    val features = statisticDf(
      spark,
      train,
      Array("goods_num", "goods_price"),
      timeCol="weekNum2",
      idCol="sku_id"
      , targetTime = 33,
      statisticTime = 30)

    features.show(false)

    val label = extractLabel(train, "sku_id", "goods_num", "weekNum2=40")
      .withColumnRenamed("sku_id", "id")

    val train2 = features.join(label, Seq("id"), "left").na.fill(0)
    val Array(trainl, testl) = train2.randomSplit(Array(0.8, 0.2))

    val gbdr = new GBTRegressor()
      .setFeaturesCol("features")
      .setLabelCol("goods_num")
      .setMaxIter(10)

    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("goods_num")
      .setRegParam(0.01)

    val re = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("goods_num")
      .setMetricName("rmse")

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(re)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)

    val cvModel = cv.fit(trainl)

    println(cvModel.bestModel.asInstanceOf[LinearRegressionModel].parent.explainParams)

    val result = cvModel.transform(testl)

    val rmse = re
      .evaluate(result)


    println(1.0 / (1 + rmse))
  }

}




