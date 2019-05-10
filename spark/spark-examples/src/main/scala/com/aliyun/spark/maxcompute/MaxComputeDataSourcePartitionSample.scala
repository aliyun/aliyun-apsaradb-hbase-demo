
package com.aliyun.spark.maxcompute

import org.apache.spark.sql.{SaveMode, SparkSession}

object MaxComputeDataSourcePartitionSample {
  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(
        """Usage: MaxComputeDataSourceSample <accessKeyId> <accessKeySecret> <maxcomputeEndpoint> <tunnelEndpoint> <project> <table> <numPartitions>
          |
          |Arguments:
          |
          |    accessKeyId      Aliyun Access Key ID.
          |    accessKeySecret  Aliyun Key Secret.
          |    maxcomputeEndpoint   查看这里 https://help.aliyun.com/document_detail/34951.html
          |    tunnelEndpoint       查看这里 https://help.aliyun.com/document_detail/34951.html
          |    project          Aliyun MaxCompute project
          |    table            Aliyun MaxCompute table
          |    numPartitions    scan的并发度
        """.stripMargin)
      System.exit(1)
    }

    //create table sparktest2 (name string) PARTITIONED BY (pt string); ; 创建一个只有一个String字段的MaxCompute表,分区为pt

    val accessKeyId = args(0)
    val accessKeySecret = args(1)
    val odpsUrl = args(2)
    val tunnelUrl = args(3)
    val project = args(4)
    val table = args(5)
    var numPartitions = 1
    if(args.length > 6)
      numPartitions = args(6).toInt

    val ss = SparkSession.builder().appName("Test Odps Read").getOrCreate()

    import ss.implicits._

    val dataSeq = (1 to 1000000).map {
      index => (index, (index-3).toString)
    }.toSeq


    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    System.out.println("*****" + table + ",before overwrite table")
    df.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", odpsUrl)
      .option("tunnelUrl", tunnelUrl)
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("partitionSpec", "pt='2018-04-01'")
      .option("allowCreateNewPartition", true)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Overwrite).save()
    System.out.println("*****" + table + ",after overwrite table, before read table")

    val readDF = ss.read
      .format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", odpsUrl)
      .option("tunnelUrl", tunnelUrl)
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "pt='2018-04-01'")
      .option("numPartitions",numPartitions).load()

    readDF.collect().foreach(println)
  }
}
