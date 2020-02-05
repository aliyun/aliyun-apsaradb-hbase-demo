package com.aliyun.spark.kafka

import com.aliyun.spark.sink.PostgreSqlSink
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 本例介绍如何使用StructuredStreaming 通过公网的方式连接阿里云kafka，然后写入数据到Postgresql。
  */
object StructredStreamingToKafkaWithPublicNet {
  def main(args: Array[String]): Unit = {
    val brokers = args(0)
    val topic = args(1)
    val url = args(2)
    val user = args(3)
    val pw = args(4)
    val table = args(5)
    val checkpoint = args(6)
    val sslLocation = args(7)
    val javaSLoactaion = args(8)

    println(s"==brokers==$brokers")
    println(s"==topic==$topic")
    println(s"==url==$url")
    println(s"==user==$user")
    println(s"==table==$table")
    println(s"==checkpoint==$checkpoint")
    println(s"==sslLocation==$sslLocation")
    println(s"==javaSLoactaion==$javaSLoactaion")


    val spark = SparkSession.builder
      .appName("kafka")
      .getOrCreate()
    import spark.implicits._

    //state string
    //city string
    //population int
    val schema = StructType(Array(
      StructField("state", StringType, true),
      StructField("city", StringType, true),
      StructField("population", LongType)))

    val encoder = RowEncoder(schema)

    //    如果 kafka-client 的版本为 2.0.0 及以上，需额外设置以下参数：
    //    ("ssl.endpoint.identification.algorithm", "");

    System.setProperty("java.security.auth.login.config", javaSLoactaion)

    val dataSets = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .option("kafka.ssl.endpoint.identification.algorithm", "")
      .option("kafka.ssl.truststore.location", sslLocation)
      .option("kafka.ssl.truststore.password", "KafkaOnsClient")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("startingOffsets", "latest")

      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map { value =>
        println($"===value====${value}")
        val valueArray = value.split(",")
        Row(valueArray(0), valueArray(1), valueArray(2).toLong)
      }(encoder)

    val pgSink = new PostgreSqlSink(url, user, pw, table)

    val result = dataSets
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpoint)
      .foreach(pgSink)
      .start()

    result.awaitTermination()
  }
}
