
package com.aliyun.spark

import java.sql.DriverManager

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.logservice.LoghubUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * 此demo为SparkStreaming 对接LogHub。
  * phoenix 建表语句：
  * CREATE TABLE IF NOT EXISTS user_event (
  * event_time BIGINT NOT NULL,
  * user_id VARCHAR NOT NULL,
  * device_id VARCHAR,
  * event_name VARCHAR,
  * prod_id VARCHAR
  * CONSTRAINT my_pk PRIMARY KEY (event_time, user_id));
  *
  */
object SparkStreamingOnLogHubToPhoenix4x {

  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(
        """Usage: LoghubSample <project> <logstore> <loghub group name> <endpoint>
          |         <access key id> <access key secret> <zkAddress>
        """.stripMargin)
      System.exit(1)
    }

    val loghubProject = args(0)
    val logStore = args(1)
    val loghubGroupName = args(2)
    val endpoint = args(3)
    val accessKeyId = args(4)
    val accessKeySecret = args(5)
    var batchInterval = Milliseconds(10 * 1000)
    val zkAddress = args(6)
    val phoenixTableName = args(7)
    if(args.length > 8) batchInterval = Milliseconds(args(8).toInt * 1000)

    val batchSize = 1000

    //需等于LogHub store的shard数
    val numReceiver = 2
    println(s"loghubProject=$loghubProject")
    println(s"logStore=$logStore")
    println(s"loghubGroupName=$loghubGroupName")
    println(s"endpoint=$endpoint")
    println(s"accessKeyId=$accessKeyId")
    println(s"accessKeySecret=$accessKeySecret")
    println(s"batchInterval=$batchInterval")
    println(s"numReceiver=$numReceiver")


    def functionToCreateContext() = {
      val conf = new SparkConf().setAppName("LoghubSample")
      val ssc = new StreamingContext(conf, batchInterval)
      val loghubStream = LoghubUtils.createStream(
        ssc,
        loghubProject,
        logStore,
        loghubGroupName,
        endpoint,
        numReceiver,
        accessKeyId,
        accessKeySecret,
        StorageLevel.MEMORY_AND_DISK)

      loghubStream.foreachRDD { rdd =>
        rdd.foreachPartition { pt =>
          // 获取Phoenix的链接
          val phoenixConn = DriverManager.getConnection("jdbc:phoenix:" + zkAddress)
          val statment = phoenixConn.createStatement()
          var i = 0
          while (pt.hasNext) {
            val value = pt.next()
            //获取的LogHub的数据是json格式的，需要进行转换
            val valueFormatted = JSON.parseObject(new String(value))
            //构造phonenix 插入语句
            val insetSql = s"upsert into $phoenixTableName values(" +
              s"${valueFormatted.getLong("event_time")}," +
              s"'${valueFormatted.getString("user_id").trim}'," +
              s"'${valueFormatted.getString("device_id").trim}'," +
              s"'${valueFormatted.getString("event_name").trim}'," +
              s"'${valueFormatted.getString("prod_id").trim}')"
            println(insetSql)
            statment.execute(insetSql)
            i = i + 1
            // 每隔batchSize行提交一次commit到Phoenix。
            if (i % batchSize == 0) {
              phoenixConn.commit()
              println(s"====finish upsert $i rows====")
            }
          }
          phoenixConn.commit()
          println(s"==last==finish upsert $i rows====")
          phoenixConn.close()
          }
      }
      ssc
    }

    val ssc = functionToCreateContext()

    ssc.start()
    ssc.awaitTermination()
  }
}
