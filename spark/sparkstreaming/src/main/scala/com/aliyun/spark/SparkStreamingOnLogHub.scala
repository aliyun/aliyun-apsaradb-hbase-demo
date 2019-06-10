
package com.aliyun.spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.logservice.LoghubUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object SparkStreamingOnLogHub {

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(
        """Usage: LoghubSample <project> <logstore> <loghub group name> <endpoint>
          |         <access key id> <access key secret>
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
    if(args.length > 6) {
      batchInterval = Milliseconds(args(6).toInt * 1000)
    }
    var checkPoint = "/tmp/SparkOnLoghub_T003/"
    if(args.length > 7) {
      checkPoint = args(7)
    }

    //需等于LogHub store的shard数
    val numReceiver = 2
    println(s"loghubProject=$loghubProject")
    println(s"logStore=$logStore")
    println(s"loghubGroupName=$loghubGroupName")
    println(s"endpoint=$endpoint")
    println(s"accessKeyId=$accessKeyId")
    println(s"accessKeySecret=$accessKeySecret")
    println(s"batchInterval=$batchInterval")
    println(s"checkPoint=$checkPoint")
    println(s"numReceiver=$numReceiver")



    def functionToCreateContext(): StreamingContext = {
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

      //通过LogHubCursorPosition.BEGIN_CURSOR指定消费Cursor的模式。
//      val loghubStream = LoghubUtils.createStream(
//        ssc,
//        loghubProject,
//        logStore,
//        loghubGroupName,
//        endpoint,
//        numReceiver,
//        accessKeyId,
//        accessKeySecret,
//        StorageLevel.MEMORY_AND_DISK,
//        LogHubCursorPosition.BEGIN_CURSOR,
//        -1,
//        false)

      loghubStream.checkpoint(batchInterval * 2).foreachRDD{rdd =>
        rdd.foreach{line =>
          val value = JSON.parseObject(new String(line))
          println(s"=${value.getString("c1")}=${value.getString("c2")}")
        }
      }
      ssc.checkpoint(checkPoint) // set checkpoint directory
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkPoint, functionToCreateContext _)

    ssc.start()
    ssc.awaitTermination()
  }
}
