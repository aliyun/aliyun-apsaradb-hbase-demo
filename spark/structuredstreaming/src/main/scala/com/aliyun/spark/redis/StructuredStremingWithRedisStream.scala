package com.aliyun.spark.redis

import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types._

object StructuredStremingWithRedisStream {

  def main(args: Array[String]): Unit = {

    //获取Redis的, redisHost:内网连接地址(host)，redisPort: 端口号(port)， redisPassword：连接密码
    val redisHost = args(0)
    val redisPort = args(1)
    val redisPassword = args(2)
    //redis侧的表名。
    var redisTableName = args(3)

    println(s"---redisHost=$redisHost-----")
    println(s"---redisPort=$redisPort-----")
    println(s"---redisPassword=$redisPassword-----")


    val spark = SparkSession
      .builder()
      .appName("StructuredStreaming on Redis")
      .config("spark.redis.host", redisHost)
      .config("spark.redis.port", redisPort)
      .config("spark.redis.auth", redisPassword)
      .getOrCreate()

    val clicks = spark
      .readStream
      .format("redis")
      .option("stream.keys", redisTableName)
      .schema(StructType(Array(
        StructField("asset", StringType),
        StructField("cost", LongType)
      )))
      .load()

//    val printStream = clicks
//      .writeStream
//      .format("console")
//      .outputMode("append")
//      .start()

    val bypass = clicks.groupBy("asset").count()

    val clickWriter = new ClickForeachWriter(redisHost, redisPort, redisPassword)

    val query = bypass
      .writeStream
      .outputMode("update")
      .foreach(clickWriter)
      .start()

    query.awaitTermination()

  }

}
