package com.lxf.jdata

import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TrainModel2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val train_3=spark.read.parquet("outputfiles/train2")

    println(train_3.count())
    val Array(train,test) =train_3.randomSplit(Array(0.8,0.2))
    val paramMap = List(
      "eta" -> 0.1, //学习率
      "gamma" -> 0, //用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
      "lambda" -> 2, //控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
      "subsample" -> 1, //随机采样训练样本
      "colsample_bytree" -> 0.8, //生成树时进行的列采样
      "max_depth" -> 7, //构建树的深度，越大越容易过拟合
      "min_child_weight" -> 5,
      "objective" -> "multi:softprob", //定义学习任务及相应的学习目标
      "eval_metric" -> "merror",
      "num_class" -> 32
    ).toMap


    val model: XGBoostModel = XGBoost.trainWithDataFrame(train_3, paramMap, round = 7, nWorkers = 7,
      useExternalMemory = true,
      featureCol = "features2",
      labelCol = "label",
      missing = 0.0f)
    //    val reslut = model.transform(test)
    //
    //    println("购买人数的准确度")
    //    println(1.0*reslut.filter($"label" > 0 and $"prediction" > 0).count()/reslut.filter($"label" > 0).count())
    //    println("预测日期的准确度：")
    //    println(1.0*reslut.filter($"label" - $"prediction" > -0.1 and  $"label" - $"prediction" < 0.1 and $"label" > 0).count()/reslut.filter($"label" > 0).count())
    //   println("总的预测准确度：")
    //    println(1.0*reslut.filter($"label" - $"prediction" > -0.1 and $"label" - $"prediction" < 0.1 ).count()/reslut.count())
  }
}
