package org.example.sparkdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}

object SparkSqlDemo {
  def main(args: Array[String]): Unit = {
    val inputPath =
      args.headOption.getOrElse("sparkdemo/src/main/resources/input/people.csv")

    val spark = SparkSession
      .builder()
      .appName("spark-sql-demo")
      .master("local[*]")
      .getOrCreate()

    try {
      val people = spark
        .read
        .option("header", value = true)
        .option("inferSchema", value = true)
        .csv(inputPath)

      people.printSchema()

      people
        .filter(col("age") >= 18)
        .groupBy(col("city"))
        .count()
        .orderBy(desc("count"), col("city"))
        .show(truncate = false)
    } finally {
      spark.stop()
    }
  }
}
