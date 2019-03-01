package com.aliyun.spark.mongodb

import org.apache.spark.sql.SparkSession

object SparkOnMongoDBSparkSparkSession {

  def main(args: Array[String]): Unit = {
    //获取MongoDB的 connectionStringURI，database 和 collection
    val connectionStringURI = args(0)
    val database = args(1)
    val collection = args(2)
    //Spark侧的表名。
    var sparkTableName = args(3)

    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport() //可选，使用hive-metastore后通过thriftServer可以查看到代码中创建的表
      .appName("scala spark on MongoDB test")
      .getOrCreate()

    //带Schema的创建方式
    var createCmd = s"""CREATE TABLE ${sparkTableName} (
                       |      id String,
                       |      name String
                       |    ) USING com.mongodb.spark.sql
                       |    options (
                       |    uri '$connectionStringURI',
                       |    database '$database',
                       |    collection '$collection'
                       |    )""".stripMargin

    println(" createCmd: \n" + createCmd)
    sparkSession.sql(createCmd)
    var querySql = "select * from " + sparkTableName + " limit 1"
    sparkSession.sql(querySql).show

    //不带Schema的创建方式
    sparkTableName = sparkTableName + "_noschema"
    createCmd = s"""CREATE TABLE ${sparkTableName} USING com.mongodb.spark.sql
                   |    options (
                   |    uri '$connectionStringURI',
                   |    database '$database',
                   |    collection '$collection'
                   |    )""".stripMargin

    println(" createCmd: \n" + createCmd)
    sparkSession.sql(createCmd)
    querySql = "select * from " + sparkTableName + " limit 1"
    sparkSession.sql(querySql).show

    sparkSession.stop()
  }
}
