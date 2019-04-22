package com.aliyun.spark.redis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkOnRedisSparkSession {

  def main(args: Array[String]): Unit = {
    //获取Redis的, redisHost:内网连接地址(host)，redisPort: 端口号(port)， redisPassword：连接密码
    val redisHost = args(0)
    val redisPort = args(1)
    val redisPassword = args(2)
    //redis侧的表名。
    var redisTableName = args(3)

    //spark conf 中配置redis信息
    val sparkConf = new SparkConf()
      .set("spark.redis.host", redisHost)
      .set("spark.redis.port", redisPort)
      .set("spark.redis.auth", redisPassword)

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    //样例数据
    val data =
      Seq(
        Person("John", 30, "60 Wall Street", 150.5),
        Person("Peter", 35, "110 Wall Street", 200.3)
      )

    //通过dataset API 写入数据
    val dfw = sparkSession.createDataFrame(data)
    dfw.write.format("org.apache.spark.sql.redis")
      .option("model", "hash")
      .option("table", redisTableName)
      .save()

    //默认方式读取redis的hash值
    var loadedDf = sparkSession.read.format("org.apache.spark.sql.redis")
      .option("table", redisTableName)
      .load()
      .cache()
    loadedDf.show(10)

    //设置infer.schema=true，spark会检索redis的Schema
    loadedDf = sparkSession.read.format("org.apache.spark.sql.redis")
      //        .option("table", redisTableName)
      .option("keys.pattern", redisTableName + ":*")
      .option("infer.schema", "true")
      .load()
    loadedDf.show(10)

    //指定Schema的方式
    loadedDf = sparkSession.read.format("org.apache.spark.sql.redis")
      .option("keys.pattern", redisTableName + ":*")
      .schema(StructType(Array(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("address", StringType),
        StructField("salary", DoubleType)
      )))
      .load()
    loadedDf.show(10)

    sparkSession.sparkContext
    sparkSession.stop()
  }

}

case class Person(name: String, age: Int, address: String, salary: Double)

