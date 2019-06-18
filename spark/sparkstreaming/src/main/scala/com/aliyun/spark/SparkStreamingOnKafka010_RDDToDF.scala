package com.aliyun.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * 此demo为SparkStreaming使用kafka010客户的实例，适用于阿里云互联网中间件->消息队列kafka。
  * 实例场景说明：kafka发送字符串，SparkStream获取字符串后按照空格拆分成单词，然后记录每个单词的总个数。
  * 本实例kafka发送的数据一行中要有3列，使用逗号分开。例如：
  * TP,city_000000111,7844358
  * XY,city_000000135,11397403
  * SR,city_000000159,18488556
  * Streaming 获取RDD数据化转换成DF，DF再注册成表，使用DF.SQL
  */
object SparkStreamingOnKafka010_RDDToDF {

  def main(args: Array[String]): Unit = {
    // kafka的broker，在阿里云互联网中间件->消息队列kafka中，topic管理或者 Consumer Group管理中的"获取接入点"中获取。格式为：ip:port,ip:port,ip:port
    val brokers = args(0)
    // kafka的 topic，在阿里云互联网中间件->消息队列kafka中，topic管理中创建。
    val topic = args(1)
    // kafka GroupID，可从kafka服务的Consumer Group 管理获取。
    val groupId = args(2)
    // SparkStreaming 批处理的时间间隔
    val batchSize = 10
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, new Duration(batchSize * 1000))
    // 添加topic。
    val topicList = topic.split(",")

    //设置kafka参数
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      //SparkStreaming 的executor消费kafka数据会创建另外一个groupID，格式为：spark-executor-${groupId}.
      //如果使用阿里云互联网中间件->消息队列kafka，需要前往阿里云互联网中间件->消息队列kafka中，
      //在Consumer Group管理中创建一个名词为spark-executor-${groupId}的group。
      "group.id" -> groupId
    )
    val locationStrategy = LocationStrategies.PreferConsistent
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicList.toSet, kafkaParams)
    //从Kafka接收数据并创建对应的DStream。
    val messages = KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy)

    //示例1
    //下面的demo用于展示，转换成DF时指定schema信息，字段类型根据定义的类型确定。
    //定义字段的类型，用于转成成DF
    val fieldsType = new StructType()
      .add("state", StringType)
      .add("city", StringType)
      .add("population", LongType)

    //获取kafka的value，按照逗号分隔，转换成Row类型RDD
    val recordValue = messages.map { record =>
      val values = record.value().split(",")
      Row(values(0), values(1), values(2).trim.toLong)
    }
    recordValue.foreachRDD { rdd =>
      //创建SparkSession
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      //转成成DF，指定转换的数据类型
      val recordValueTable = spark.createDataFrame(rdd, fieldsType)
      //注册成表
      recordValueTable.createOrReplaceTempView("record_table")
      //对标使用SQL查询
      val valuesLimit = spark.sql("select state, city, population from record_table limit 10")
      valuesLimit.show()
    }

    //示例2
    //下面的demo用于展示，转换成DF时不指定Schema信息，默认字段类型是String
    //获取kafka的value，按照逗号分隔，转换成元组
    val recordValue02 = messages.map { record =>
      val values = record.value().split(",")
      (values(0), values(1), values(2).trim.toLong)
    }
    recordValue02.foreachRDD { rdd =>
      //创建SparkSession
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      //导入隐士转换，为RDD.toDF准备。
      import spark.implicits._
      //转换成DF, 字段默认为string类型
      val recordValueTable02 = rdd.toDF("state", "city","population")
      //注册成表
      recordValueTable02.createOrReplaceTempView("record_table02")
      //对标使用SQL查询
      val valuesLimit02 = spark.sql("select state as state02, city, population from record_table02 limit 10")
      valuesLimit02.show()
    }

    ssc.start()
    try
      ssc.awaitTermination()
    catch {
      case e: InterruptedException =>

    }
  }
}