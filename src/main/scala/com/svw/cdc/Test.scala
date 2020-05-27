package com.svw.cdc

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

object Test {

  /*val cdcSchema = StructType(
    StructField("id", StringType, nullable = false) ::
      StructField("first_name", StringType, nullable = true) ::
      StructField("last_name", StringType, nullable = true) ::
      StructField("email", StringType, nullable = true) :: Nil
  )

  case class Cdc(
                  id: String,
                  first_name: String,
                  last_name: String,
                  email: String
                )*/

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("cdc-test").config(new SparkConf()).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> "172.20.98.138:9091"
      , "auto.offset.reset" -> "latest"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "group.id" -> ""
      , "auto.offset.reset" -> "earliest"
    )

    val topic = Array("dbserver1.inventory.customers")
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
//      cdcDF.printSchema()
      val plDF = cdcDF.select(explode(cdcDF("payload"))).toDF("pl")
      val beforeDF = plDF.select(explode(plDF("before"))).toDF("before")
      val afterDF = plDF.select(explode(plDF("after"))).toDF("after")
      val beforeVal = beforeDF.select("before.id","before.first_name","before.last_name","before.email")
      val afterVal = afterDF.select("after.id","after.first_name","after.last_name","after.email")
      beforeVal.show()
      afterVal.show()
//      println("print---------------------------------df")
//      println(cdcDF.describe())
//      println(cdcDF.head())
      kuduContext.upsertRows(cdcDF, "impala::tmp.tb_cdc")
    })
    /*dStream.foreachRDD(rdd => {
      //将rdd数据重新封装为Rdd[cdc]
      println("print---------------------------------in rdd")
      val newrdd = rdd.map(line => {
        val jsonObj = JSON.parseFull(line.value())
        val map: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
        Cdc(
          map("id").asInstanceOf[String],
          map("first_name").asInstanceOf[String],
          map("last_name").asInstanceOf[String],
          map("email").asInstanceOf[String]
        )
      })

      val cdcDF = spark.sqlContext.createDataFrame(newrdd)
      println("print---------------------------------df")
      println(cdcDF.describe())
      println(cdcDF.head())
      kuduContext.upsertRows(cdcDF, "impala::tmp.tb_cdc")
    })*/
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
