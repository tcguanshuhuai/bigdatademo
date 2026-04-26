package org.example.sparkdemo

import org.apache.spark.sql.SparkSession

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val inputPath =
      args.headOption.getOrElse("sparkdemo/src/main/resources/input/words.txt")

    val spark = SparkSession
      .builder()
      .appName("spark-word-count-demo")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._

      val counts = spark
        .read
        .textFile(inputPath)
        .flatMap(_.toLowerCase.split("\\W+"))
        .filter(_.nonEmpty)
        .groupByKey(word => word)
        .count()
        .orderBy("key")

      counts.collect().foreach { case (word, count) =>
        println(s"$word -> $count")
      }
    } finally {
      spark.stop()
    }
  }
}
