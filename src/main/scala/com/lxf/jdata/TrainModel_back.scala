package com.lxf.jdata

import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoder, VectorAssembler}
import org.apache.spark.sql.SparkSession

object TrainModel_back {
  def main(args: Array[String]): Unit = {
//    SetLogger
    val conf=new SparkConf().setMaster("local[*]").set("spark.executor.memory","6g")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val train_1 = spark.read.parquet("outputfiles/train").repartition(1).cache()

    val minus = train_1.filter($"label" === 0).sample(false, 0.01, 4)
    val plus = train_1.filter($"label" > 0)
    val temp = minus
        .union(plus)

    val temp1=new OneHotEncoder().setInputCol("age").setOutputCol("age_vec").transform(temp).drop("age")
    val temp2=new OneHotEncoder().setInputCol("sex").setOutputCol("sex_vec").transform(temp1).drop("sex")
    val temp3=new OneHotEncoder().setInputCol("userGrade").setOutputCol("userGrade_vec").transform(temp2).drop("userGrade")
    val temp4=new OneHotEncoder().setInputCol("cate").setOutputCol("cate_vec").transform(temp3).drop("cate")

    val cols = temp4.columns.filter(!Array("label","productId","userId").contains(_))
    val temp5=new VectorAssembler().setInputCols(cols).setOutputCol("features").transform(temp4)
    val train_2=temp5.select("userId","productId","features","label")

    val model2 = new MinMaxScaler().setInputCol("features").setOutputCol("features2").fit(train_2)
    val train_3=model2.transform(train_2).drop("features")


    val Array(train,test) =train_3.repartition(1).randomSplit(Array(0.8,0.2))
    val(maxDepth , numRound , nworker )=(7,7,7)
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
      "num_class" -> 31
    ).toMap

    train_3.repartition(1).write.mode("overwrite").parquet("outputfiles/train2")


//    val model: XGBoostModel = XGBoost.trainWithDataFrame(train, paramMap, numRound, nworker,
//      useExternalMemory = true,
//      featureCol = "features2",
//      labelCol = "label",
//      missing = 0.0f)

  }
}
