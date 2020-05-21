package com.svw.cdc

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("cdc-test")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.20.98.138:9092")
      .option("subscribe", "dbserver1.inventory.customers")
      .load()
    import spark.implicits._
    val lines = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    println(lines)
    val lv = lines.map(_.toString().split(","))
    print(lv)


    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.20.98.138:9092")
      .option("topic", "downstream")
      .option("checkpointLocation", "/tmp/cdc")
      .start()

    query.awaitTermination()
    spark.close()
  }
}
