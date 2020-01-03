package com.aliyun.spark.manageroffset

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.util.Random

/**
  * this demo show how to manager the kafka offset back write to kafka
  */
object SparkWriteKafkaOffsetBackToKafka {

  def main(args: Array[String]): Unit = {
    // kafka的broker，在阿里云互联网中间件->消息队列kafka中，topic管理或者 Consumer Group管理中的"获取接入点"中获取。格式为：ip:port,ip:port,ip:port
    val brokers = args(0)
    // kafka的 topic，在阿里云互联网中间件->消息队列kafka中，topic管理中创建。
    val topic = args(1)
    // kafka GroupID，可从kafka服务的Consumer Group 管理获取。
    val groupId = args(2)
    // SparkStreaming 批处理的时间间隔
    val duration = args(3).toInt * 1000

    //通过spark.streaming.kafka.maxRatePerPartition参数限流
    //    * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
    //    *  of messages
    //    * per second that each '''partition''' will accept.
    val sparkConf = new SparkConf().setAppName("KafkaOffsetManager")
      .set("spark.streaming.kafka.maxRatePerPartition", "2000")
    val ssc = new StreamingContext(sparkConf, new Duration(duration))

    val messages = init_kafka(brokers,groupId,topic,ssc)

    // 处理数据
    messages.foreachRDD(rdd => {
      //获取当前的offset
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach{of =>
        println(s"The partition is: ${of.partition}, " +
          s"offset from: ${of.fromOffset} and end: ${of.untilOffset}, " +
          s"total count is: ${of.count()}")
      }

      rdd.foreachPartition(pt => println(s"the pt count is:  ${pt.size}"))
      
      val start = System.currentTimeMillis()
      //commit offset 到kafka
      messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      val end = System.currentTimeMillis()
      println(s"commit offset back to kafka cost: ${end - start} milliseconds")
    })

    //启动SparkStreaming
    ssc.start()
    try
      ssc.awaitTermination()
    catch {
      case e: InterruptedException => {
        println(e)
      }
    }
  }

  def init_kafka(brokers:String, groupId: String, topic:String, ssc:StreamingContext): DStream[ConsumerRecord[String,String]] = {
    // 添加topic。
    val topicList = topic.split(",")

    //设置kafka参数
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      //通过kafka 参数限流读取的行数
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "5000",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      //消费kakfa的数据的offset策略。
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    //通过kafka参数限制读取的大小。
//    ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG -> "1024"
//    ConsumerConfig.FETCH_MIN_BYTES_CONFIG -> "20000"
//    ConsumerConfig.FETCH_MAX_BYTES_CONFIG -> "4000"

    val locationStrategy = LocationStrategies.PreferConsistent
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicList.toSet, kafkaParams)
    //从Kafka接收数据并创建对应的DStream。
    val messages = KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy)
    messages
  }
}
