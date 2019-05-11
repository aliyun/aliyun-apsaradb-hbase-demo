
package com.aliyun.spark

import java.util

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.logservice.LoghubUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object SparkStreamingOnLogHubToHBase {

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
    if(args.length > 7) {
      batchInterval = Milliseconds(args(7).toInt * 1000)
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
    println(s"numReceiver=$numReceiver")


    def functionToCreateContext(): StreamingContext = {
      val conf = new SparkConf().setAppName("LoghubSample")
      val ssc = new StreamingContext(conf, batchInterval)
      val hbaseBroadCastHBaseConnection: Broadcast[BroadCastHBaseConnection] = ssc.sparkContext.broadcast(BroadCastHBaseConnection(zkAddress))
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

      loghubStream.foreachRDD{rdd =>
        rdd.foreachPartition{pt =>
          val hbaseTable = "loghubtest"
          val cf = "cf1".getBytes()
          val qualifilter = "name".getBytes()
          val hbaseConnection = hbaseBroadCastHBaseConnection.value
          val table = hbaseConnection.getTable(TableName.valueOf(hbaseTable))
          val puts = new util.ArrayList[Put]()
          var i = 0
          pt.foreach{value =>
            val valueFormatted = JSON.parseObject(new String(value))
            val put = new Put(valueFormatted.getString("c1").getBytes)
            put.addColumn(cf, qualifilter, valueFormatted.getString("c2").getBytes)
            puts.add(put)
            i = i + 1
            if (i % 1000 == 0) {
              table.put(puts)
              puts.clear()
              i = 0
              println(s" finish put the table $hbaseTable $i records")
            }
          }
          table.put(puts)
          puts.clear()
          table.close()
          println(s" finish put the table $hbaseTable $i records")
        }


        rdd.foreach{line =>
          val value = JSON.parseObject(new String(line))
          println(s"=${value.getString("c1")}=${value.getString("c2")}")
        }
      }
      ssc
    }

    val ssc = functionToCreateContext()

    ssc.start()
    ssc.awaitTermination()
  }
}
