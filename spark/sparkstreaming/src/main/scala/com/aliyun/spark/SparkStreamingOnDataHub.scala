package com.aliyun.spark

import com.aliyun.datahub.model.RecordEntry
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.datahub.DatahubUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object SparkStreamingOnDataHub {
  def main(args: Array[String]): Unit = {

    val endpoint = args(0)
    // RAM访问控制中的AccessKeyID
    val accessKeyId = args(1)
    // RAM访问控制中的AccessKeySecret
    val accessKeySecret = args(2)
    // datahub 的 订阅ID
    val subId = args(3)
    // datahub 的 project 名称
    val project = args(4)
    // datahub 的 topic 名称
    val topic = args(5)
    val batchInterval = Milliseconds(10 * 1000)

    var checkpoint = "/tmp/SparkOnDatahubReliable_T001/"
    if (args.length >= 7) {
      checkpoint = args(6)
    }
    var shardId = "0"
    if (args.length >= 8) {
      shardId = args(7).trim
    }

    println(s"=====project=${project}===topic=${topic}===batchInterval=${batchInterval.milliseconds / 1000}=====")

    def functionToCreateContext(): StreamingContext = {
      val conf = new SparkConf().setAppName("Test Datahub")
      //设置使用Reliable DataReceiver
      conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
      val ssc = new StreamingContext(conf, batchInterval)
      ssc.checkpoint(checkpoint)

      var datahubStream: DStream[String] = null
      if (!shardId.isEmpty) {
        datahubStream = DatahubUtils.createStream(
          ssc,
          project,
          topic,
          subId,
          accessKeyId,
          accessKeySecret,
          endpoint,
          shardId,
          read,
          StorageLevel.MEMORY_AND_DISK_SER_2)
      } else {
        datahubStream = DatahubUtils.createStream(
          ssc,
          project,
          topic,
          subId,
          accessKeyId,
          accessKeySecret,
          endpoint,
          read,
          StorageLevel.MEMORY_AND_DISK_SER_2)
      }

      datahubStream.foreachRDD { rdd =>
        //注意，测试环境小数据量使用了，rdd.collect(). 真实环境请慎用。
        rdd.collect().foreach(println)
//        rdd.foreach(println)
      }
      ssc
    }

    val ssc = StreamingContext.getActiveOrCreate(checkpoint, functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()
  }

  def read(record: RecordEntry): String = {
    s"${record.getString(0)},${record.getString(1)}"
  }
}


