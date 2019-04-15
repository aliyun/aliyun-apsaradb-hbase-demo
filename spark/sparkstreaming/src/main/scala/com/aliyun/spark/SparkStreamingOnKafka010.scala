package com.aliyun.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * 此demo为SparkStreaming使用kafka010客户的实例，适用于阿里云互联网中间件->消息队列kafka。
  * 实例场景说明：kafka发送字符串，SparkStream获取字符串后按照空格拆分成单词，然后记录每个单词的总个数。
  * 运行命令：
  * spark-submit --master yarn --class com.aliyun.spark.SparkStreamingOnKafka010 /opt/jars/test/sparkstreaming-kafka010-0.0.1-SNAPSHOT.jar
  * ip1:9092,ip2:9092,ip3:9092 topic groupId
  */
object SparkStreamingOnKafka010 {

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
    val kafkaParams = Map[String,String] (
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
    //对获取的数据按照空格拆分单词
    val words = messages.transform(rdd => {
      rdd.flatMap(line => line.value().split(" "))
    }).map(word => (word, 1L))
    //对单词计总数
    val wordCounts = words.reduceByKey(_ + _)
    val totalCounts = wordCounts.updateStateByKey(updateFunc)
    //打印单词总个数。
    wordCounts.print()
    //启动SparkStreaming
    ssc.start()
    try
      ssc.awaitTermination()
    catch {
      case e: InterruptedException =>

    }
  }

  def updateFunc(values : Seq[Long], state : Option[Long]) : Option[Long] =
    Some(values.sum + state.getOrElse(0L))
}
