package com.aliyun.spark

import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * 此demo为SparkStreaming使用kafka010客户的实例，适用于阿里云互联网中间件->消息队列kafka。
  * 实例场景说明：kafka发送字符串，SparkStream获取字符串后按照空格拆分后入库hbase。
  */
object SparkStreamingOnKafka010ToHBase {

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
    //HBase集群的ZK链接地址。//HBase集群的ZK链接地址。使用时请把此路径替换为你自己的HBase集群的zk访问地址。
    //格式为：xxx-002.hbase.rds.aliyuncs.com:2181,xxx-001.hbase.rds.aliyuncs.com:2181,xxx-003.hbase.rds.aliyuncs.com:2181
    val zkAddress = args(3)

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
    val words = messages.transform { rdd =>
      rdd.map {line =>
        line.value().split("\\s+")
      }
    }.foreachRDD { lineArray =>
      lineArray.foreachPartition { dataPartition =>
        val hbaseTableName = "mytable"
        val cf = "cf1"
        val qualifier1 = "col1".getBytes
        val qualifier2 = "col2".getBytes
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkAddress)
        var conn = ConnectionFactory.createConnection(hbaseConf)
        var table = conn.getTable(TableName.valueOf(hbaseTableName))
        val puts = new util.ArrayList[Put]()
        var i = 0
        while (dataPartition.hasNext) {
          val kv = dataPartition.next()
          val put = new Put(kv(0).getBytes)
          put.addColumn(cf.getBytes(), qualifier1, kv(1).getBytes)
          put.addColumn(cf.getBytes(), qualifier2, kv(2).getBytes)
          puts.add(put)
        }
        table.put(puts)
        println("=====insert into hbase " + i + " rows ===========")
        table.close()
        conn.close()
      }
    }

    //启动SparkStreaming
    ssc.start()
    try
      ssc.awaitTermination()
    catch {
      case e: InterruptedException =>

    }
  }
}
