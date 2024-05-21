package org.sguan.example

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object LinearRegressionExample {

  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession
      .builder
//      .master("local[2]")
      .appName("LinearRegressionExample")
      //.master("local") 如果在IDE中运行，加上这句控制使用本地模式
      .getOrCreate()

//    var dataPath = "/tmp/exampledata"
    var dataPath = "hdfs://ns/test/exampledata"


    // 加载数据集，这是一个LibSVM格式的数据集
    val training = spark.read.format("libsvm")
      .load(dataPath)

    // 创建线性回归模型实例，并设置参数
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // 使用数据集训练模型
    val lrModel = lr.fit(training)
    lrModel.save("hdfs://ns/test/model")

    // 打印回归模型的系数和截距
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // 关闭SparkSession
    spark.stop()
  }
}