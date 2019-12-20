package com.aliyun.spark.streamingjoin

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, StreamingContext}

/**
  * 此demo为SparkStreaming 两个Streaming join的样例，其中一个Streaming会使用到window。
  */
object SparkStreamingJoinTwoStreams {

  def main(args: Array[String]): Unit = {
    // kafka的broker，在阿里云互联网中间件->消息队列kafka中，topic管理或者 Consumer Group管理中的"获取接入点"中获取。格式为：ip:port,ip:port,ip:port
    val brokers1 = args(0)
    val brokers2 = args(1)
    // kafka的 topic，在阿里云互联网中间件->消息队列kafka中，topic管理中创建。
    val topic1 = args(2)
    val topic2 = args(3)
    // kafka GroupID，可从kafka服务的Consumer Group 管理获取。
    val groupId1 = args(4)
    val groupId2 = args(5)
    // SparkStreaming 批处理的时间间隔
    val batchSize1 = 10
    val batchSize2 = 10
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, new Duration(batchSize1 * 1000))
    // 添加topic。
    val topicList1 = topic1.split(",")
    val topicList2 = topic2.split(",")


    //设置kafka参数
    val kafkaParams1 = Map[String,String] (
      "bootstrap.servers" -> brokers1,
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      //SparkStreaming 的executor消费kafka数据会创建另外一个groupID，格式为：spark-executor-${groupId}.
      //如果kakfa组件有对groupid鉴权的话，需要创建一个名为spark-executor-${groupId}的group。
      "group.id" -> groupId1
    )

    val kafkaParams2 = Map[String,String] (
      "bootstrap.servers" -> brokers2,
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      //SparkStreaming 的executor消费kafka数据会创建另外一个groupID，格式为：spark-executor-${groupId}.
      //如果kakfa组件有对groupid鉴权的话，需要创建一个名为spark-executor-${groupId}的group。
      "group.id" -> groupId2
    )

    val locationStrategy = LocationStrategies.PreferConsistent
    val consumerStrategy1 = ConsumerStrategies.Subscribe[String, String](topicList1.toSet, kafkaParams1)
    val consumerStrategy2 = ConsumerStrategies.Subscribe[String, String](topicList2.toSet, kafkaParams2)

    //从Kafka接收数据并创建对应的DStream。
    val messages1 = KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy1)
    //从Kafka接收数据并创建对应的DStream。
    val messages2 = KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy2)

    //获取数据kafka1
    val words1 = messages1.transform{ rdd => {
      rdd.map{ line =>
        val lineArray = line.value().split(",")
        //数据格式需要是(key, values)的形式，然后才能join
        (lineArray(0), (lineArray(1), lineArray(2).toLong))
      }
    }}

    //获取数据kafka2
    val words2 = messages2.transform{ rdd => {
      rdd.map{ line =>
        val lineArray = line.value().split(",")
        //数据格式需要是(key, values)的形式，然后才能join
        (lineArray(0), (lineArray(1), lineArray(2).toLong))
      }
    }}

    //设置窗口的时间为1分钟，如果批次是10秒的话，会缓存：1分钟/10s = 6 个批次的数据
    val words1Window = words1.window(Minutes(1))
    words1Window.foreachRDD(rdd => println("Stream1 rdd.count=" + rdd.count()))

    //两个Streaming join并打印结果。
    val joinedStream = words2.join(words1Window).map(value => (value._1, value._2._1))
    joinedStream.foreachRDD(rdd =>
      println("Two streams join result count=" + rdd.count())
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
