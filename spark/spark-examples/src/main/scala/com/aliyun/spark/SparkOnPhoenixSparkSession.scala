package com.aliyun.spark

import org.apache.spark.sql.SparkSession

/**
  * 运行命令
  * spark-submit --master yarn --class com.aliyun.spark.SparkOnPhoenixSparkSession /opt/spark-examples-0.0.1-SNAPSHOT.jar zkAddress
  */
object SparkOnPhoenixSparkSession {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport() //使用hive-metastore后通过beenline可以查看到代码中创建的表。
      .appName("scala spark on Phoenix test")
      .getOrCreate()

    //Spark侧的表名。
    val sparkTableName = "spark_phoenix"
    //Phoenix侧的表名，需要在Phoenix侧提前创建。Phoenix表创建可以参考：https://help.aliyun.com/document_detail/53716.html?spm=a2c4g.11186623.4.2.4e961ff0lRqHUW
    val phoenixTableName = "us_population"
    //HBase集群的ZK链接地址。//HBase集群的ZK链接地址。使用时请把此路径替换为你自己的HBase集群的zk访问地址。
    //格式为：xxx-002.hbase.rds.aliyuncs.com,xxx-001.hbase.rds.aliyuncs.com,xxx-003.hbase.rds.aliyuncs.com:2181
    val zkAddress = args(0)
    val createCmd = "CREATE TABLE " +
      sparkTableName +
      " USING org.apache.phoenix.spark\n" +
      "OPTIONS (\n" +
      "  'zkUrl' '" + zkAddress + "',\n" +
      "  'table' '" + phoenixTableName + "'\n" +
      ")"
    println(" createCmd: \n" + createCmd)
    sparkSession.sql(createCmd)
    val querySql = "select * from " + sparkTableName + " limit 1"
    sparkSession.sql(querySql).show
    sparkSession.stop()
  }
}
