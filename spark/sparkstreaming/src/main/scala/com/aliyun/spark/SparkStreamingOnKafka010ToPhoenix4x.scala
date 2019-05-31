package com.aliyun.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * 此demo为SparkStreaming使用kafka010客户的实例，适用于阿里云互联网中间件->消息队列kafka。
  * 实例场景说明：kafka发送字符串，SparkStream获取字符串后按照空格拆分后入库Phoenix。
  * 消息内容的参考 [[com.aliyun.kafka.KafkaProducerDemoForTableData_java]]
  * phoenix 建表语句：
  * CREATE TABLE IF NOT EXISTS us_population (
  *   state CHAR(2) NOT NULL,
  *   city VARCHAR NOT NULL,
  *   population BIGINT
  * CONSTRAINT my_pk PRIMARY KEY (state, city));
  *
  * spark对接kafka可参考指导文档：https://help.aliyun.com/document_detail/114567.html?spm=a2c4g.11174283.6.600.71bf3c2eVsUNgm
  */

object SparkStreamingOnKafka010ToPhoenix4x {

  def main(args: Array[String]): Unit = {
    //HBase集群的ZK链接地址。//HBase集群的ZK链接地址。使用时请把此路径替换为你自己的HBase集群的zk访问地址。
    //格式为：xxx-002.hbase.rds.aliyuncs.com:2181,xxx-001.hbase.rds.aliyuncs.com:2181,xxx-003.hbase.rds.aliyuncs.com:2181
    val zkAddress = args(0)
    // kafka的broker，在阿里云互联网中间件->消息队列kafka中，topic管理或者 Consumer Group管理中的"获取接入点"中获取。格式为：ip:port,ip:port,ip:port
    val brokers = args(1)
    // kafka的 topic，在阿里云互联网中间件->消息队列kafka中，topic管理中创建。
    val topic = args(2)
    // kafka GroupID，可从kafka服务的Consumer Group 管理获取。
    val groupId = args(3)
    // SparkStreaming 批处理的时间间隔
    val duration = args(4).toInt * 1000
    // 入库到Phoenix每次commit的数据行数
    val batchSize = args(5).toInt
    val phoenixTableName = args(6)
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, new Duration(duration))
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
    val words = messages.transform { rdd =>
      rdd.map {line =>
        println(s"==0== words = $line")
        //逗号分隔
        val words = line.value().split(",")
        //空格分隔
//        line.value().split("\\s+")
        words
      }
    }.foreachRDD { lineArray =>
      lineArray.foreachPartition { dataPartition =>
        val phoenixConn = DriverManager.getConnection("jdbc:phoenix:" + zkAddress)
        val statment = phoenixConn.createStatement()
        var i = 0
        while (dataPartition.hasNext) {
          val kv = dataPartition.next()
          statment.execute(s"upsert into $phoenixTableName values('${kv(0)}','${kv(1)}', ${kv(2)})")
          i = i + 1
          if (i % batchSize == 0) {
            phoenixConn.commit()
            println(s"==1==finish upsert $i rows")
          }
        }
        phoenixConn.commit()
        println(s"==2==finish upsert $i rows")
        phoenixConn.close()
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
