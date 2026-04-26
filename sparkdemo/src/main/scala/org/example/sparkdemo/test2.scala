package org.example.sparkdemo


import org.apache.spark.SparkContext

object test2 {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[3]", "wordcount")
    val data = sc.parallelize(List("a c", "a b", "b c", "b d", "c d"), 2)
    val wordcount = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).reduceByKey(_ + _)

    val data2 = sc.parallelize(List("a c", "a b", "b c", "b d", "c d"), 2)
    val wordcount2 = data2.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).reduceByKey(_ + _)
    println(wordcount.join(wordcount2).toDebugString)
    for (elem <- wordcount.join(wordcount2).collect()) {
      println(elem)
    }
  }
}