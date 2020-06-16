package com.svw.cdc

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * root
 * |-- payload: struct (nullable = true)
 * |    |-- after: struct (nullable = true)
 * |    |    |-- email: string (nullable = true)
 * |    |    |-- first_name: string (nullable = true)
 * |    |    |-- id: long (nullable = true)
 * |    |    |-- last_name: string (nullable = true)
 * |    |-- before: struct (nullable = true)
 * |    |    |-- email: string (nullable = true)
 * |    |    |-- first_name: string (nullable = true)
 * |    |    |-- id: long (nullable = true)
 * |    |    |-- last_name: string (nullable = true)
 * |    |-- op: string (nullable = true)
 * |    |-- source: struct (nullable = true)
 * |    |    |-- db: string (nullable = true)
 * |    |    |-- file: string (nullable = true)
 * |    |    |-- gtid: string (nullable = true)
 * |    |    |-- name: string (nullable = true)
 * |    |    |-- pos: long (nullable = true)
 * |    |    |-- row: long (nullable = true)
 * |    |    |-- server_id: long (nullable = true)
 * |    |    |-- snapshot: string (nullable = true)
 * |    |    |-- table: string (nullable = true)
 * |    |    |-- thread: long (nullable = true)
 * |    |    |-- ts_sec: long (nullable = true)
 * |    |-- ts_ms: long (nullable = true)
 * |    |-- name: string (nullable = true)
 * |    |-- optional: boolean (nullable = true)
 * |    |-- type: string (nullable = true)
 * |    |-- version: long (nullable = true)
 */
object ChangedDataCapture {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("cdc-test").config(new SparkConf()).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.160.240.244:9092",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group"
    )

    val topic = Array("cdc.cdc.tb_cdc")
    val kudu_tb_name = "impala::tmp.tb_cdc"
    val kuduMaster = "svlhdp004.csvw.com:7051,svlhdp005.csvw.com:7051,svlhdp006.csvw.com:7051"

    val dStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topic, kafkaParams))

    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)

    if (!kuduContext.tableExists("impala::tmp.tb_cdc")) {
      println("The Kudu Table :{impala::tmp.tb_cdc} does not exist.")
    }
    // 处理流 Kafka
    dStream.map(record => record.value()).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        // 解析json消息并转化为DataFrame
        val cdcDF = spark.read.json(spark.createDataset(rdd))
        try {
          // 抽取msg里的"op"，用于判断该msg是哪种操作
          val operation = cdcDF.select("payload.op").first().getString(0)
          println("-----------------" + operation + "------------------------------")
          if (operation == "u" || operation == "c") {
            // 抽取msg里的相关业务字段并转化为DataFrame
            val afterDF = cdcDF.select("payload.after.id", "payload.after.firstname", "payload.after.lastname", "payload.after.email")
              .toDF("id", "firstname", "lastname", "email")
            println("-----------------" + afterDF + "------------------------------")
            // 更新数据到Kudu相应的表
            kuduContext.upsertRows(afterDF, kudu_tb_name)
          }
        } catch {
          case exception: AnalysisException =>
            println("oooops,crashed")
        }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}