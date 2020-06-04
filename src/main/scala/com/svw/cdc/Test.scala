package com.svw.cdc

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
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
object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("cdc-test").config(new SparkConf()).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> "10.160.240.244:9092"
      , "auto.offset.reset" -> "latest"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "group.id" -> "test-consumer-group"
    )

    val topic = Array("cdc.cdc.tb_cdc")
    val kuduMaster = "svlhdp004.csvw.com:7051,svlhdp005.csvw.com:7051,svlhdp006.csvw.com:7051"
    println("print---------------------------------start conn stream")
    val dStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topic, kafkaParams))
    println("print---------------------------------connected")
    println("print---------------------------------stream")
    println(dStream)
    println(dStream.id)
    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)

    if (!kuduContext.tableExists("impala::tmp.tb_cdc")) {
      println("create Kudu Table :{impala::tmp.tb_cdc}")
    }
    else {
      println("The table {impala::tmp.tb_cdc} exists")
    }


    /*val testDF = spark.read.format("json").load("/tmp/testcdc.json")

    testDF.printSchema()
    println("---------------------------------id")*/

    /*val operation = testDF.select("payload.op").first().getString(0)
    println(operation)
    if (operation == "u" || operation == "c"){
      val beforeDF = testDF.select("payload.before.id","payload.before.first_name","payload.before.last_name","payload.before.email").toDF("id","firstname","lastname","email")
      beforeDF.show()
      val afterDF = testDF.select("payload.after.id","payload.after.first_name","payload.after.last_name","payload.after.email").toDF("id","firstname","lastname","email")
      afterDF.show()

      kuduContext.upsertRows(afterDF, "impala::tmp.tb_cdc")
    }*/

    /*if (!kuduContext.tableExists("tb_cdc")) {
      println("create Kudu Table :{tb_cdc}")
      val createTableOptions = new CreateTableOptions()
      createTableOptions.setRangePartitionColumns(List("id"))
      kuduContext.createTable("tb_cdc", cdcSchema, Seq("id"), createTableOptions)
    }*/
    dStream.map(record => record.value()).foreachRDD(rdd => {
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val cdcDF = spark.read.json(spark.createDataset(rdd))
      cdcDF.printSchema()
      /*val operation = cdcDF.select("payload.op").first().getString(0)
      println(operation)*/
      //      if (operation == "u" || operation == "c"){
      val beforeDF = cdcDF.select("payload.before.id","payload.before.first_name","payload.before.last_name","payload.before.email").toDF("id","firstname","lastname","email")
      beforeDF.show()
      val afterDF = cdcDF.select("payload.after.id","payload.after.first_name","payload.after.last_name","payload.after.email").toDF("id","firstname","lastname","email")
      afterDF.show()

      kuduContext.upsertRows(afterDF, "impala::tmp.tb_cdc")
      //      }

      /*val plDF = cdcDF.select(explode(cdcDF("payload"))).toDF("pl")
      val beforeDF = plDF.select(explode(plDF("before"))).toDF("before")
      val afterDF = plDF.select(explode(plDF("after"))).toDF("after")
      val beforeVal = beforeDF.select("before.id","before.first_name","before.last_name","before.email")
      val afterVal = afterDF.select("after.id","after.first_name","after.last_name","after.email")
      beforeVal.show()
      afterVal.show()
      kuduContext.upsertRows(afterVal, "impala::tmp.tb_cdc")*/
    })

    ssc.start()
    ssc.awaitTermination()


    /*    val spark = SparkSession
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
        val query2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          .writeStream
          .format("console")
          .start()

        val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          .writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "172.20.98.138:9092")
          .option("topic", "downstream")
          .option("checkpointLocation", "/tmp/cdc")
          .start()

        query.awaitTermination()
        query2.awaitTermination()
        spark.close()*/
  }
}
