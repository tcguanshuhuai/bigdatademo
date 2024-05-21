package org.sguan.example.producor

import org.apache.spark.sql.SparkSession

object IcebergTest1 {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.sql.catalog.hive_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hive_prod.type", "hive")
      .config("spark.sql.catalog.hive_prod.uri", "thrift://node01:9083")
      //    .config("iceberg.engine.hive.enabled", "true")
      //指定hadoop catalog，catalog名称为hadoop_prod
      .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://node01:8020/warehouse/spark-iceberg")
      .getOrCreate()


    val sc = ss.sparkContext

    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://ns")
    sc.hadoopConfiguration.set("dfs.nameservices", "ns")
    sc.hadoopConfiguration.set("dfs.ha.namenodes.ns", "nn1,nn2")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ns.nn1", "node01:8020")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ns.nn2", "node02:8020")
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.ns", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")

    ss.sql("select * from hive_prod.iceberg_db.tb1").show
    
    

  }

}
