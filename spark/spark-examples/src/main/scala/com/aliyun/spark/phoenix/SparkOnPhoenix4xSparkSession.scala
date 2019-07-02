package com.aliyun.spark.phoenix

import org.apache.spark.sql.SparkSession

/**
  * 本实例适用于Phoenix 4.x版本
  */
object SparkOnPhoenix4xSparkSession {
  def main(args: Array[String]): Unit = {
    //HBase集群的ZK链接地址。
    //格式为：xxx-002.hbase.rds.aliyuncs.com,xxx-001.hbase.rds.aliyuncs.com,xxx-003.hbase.rds.aliyuncs.com:2181
    val zkAddress = args(0)
    //Phoenix侧的表名，需要在Phoenix侧提前创建。Phoenix表创建可以参考：https://help.aliyun.com/document_detail/53716.html?spm=a2c4g.11186623.4.2.4e961ff0lRqHUW
    val phoenixTableName = args(1)
    //Spark侧的表名。
    val sparkTableName = args(2)

    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport() //使用hive-metastore后通过beenline可以查看到代码中创建的表。
      .appName("scala spark on Phoenix test")
      .getOrCreate()

    //如果存在的话就删除表
    sparkSession.sql(s"drop table if exists $sparkTableName")

    val createCmd = "CREATE TABLE " +
      sparkTableName +
      " USING org.apache.phoenix.spark\n" +
      "OPTIONS (\n" +
      "  'zkUrl' '" + zkAddress + "',\n" +
      "  'table' '" + phoenixTableName + "'\n" +
      ")"
//    println(" createCmd: \n" + createCmd)
    sparkSession.sql(createCmd)
    val querySql = "select * from " + sparkTableName + " limit 1"
    sparkSession.sql(querySql).show
    sparkSession.stop()
  }
}
