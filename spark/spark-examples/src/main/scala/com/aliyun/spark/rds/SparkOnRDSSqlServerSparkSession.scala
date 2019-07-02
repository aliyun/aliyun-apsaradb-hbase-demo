package com.aliyun.spark.rds

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkOnRDSSqlServerSparkSession {

  def main(args: Array[String]): Unit = {
    //获取RDS的 url、database、tableName、登录RDS数据库的user和password
    val url = args(0)
    val database = args(1)
    val jdbcConnURL = s"jdbc:sqlserver://$url;DatabaseName=$database"
    val schemaName = args(2)
    val tableName = args(3)
    val user = args(4)
    val password = args(5)

    //Spark侧的表名。
    var sparkTableName = args(6)

    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport() //可选，使用hive-metastore后通过thriftServer可以查看到代码中创建的表
      .appName("scala spark on RDS test")
      .getOrCreate()

    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    //如果存在的话就删除表
    sparkSession.sql(s"drop table if exists $sparkTableName")

    //Sql方式，Spark会映射RDS中表的Schema。
    val createCmd =
      s"""CREATE TABLE ${sparkTableName} USING org.apache.spark.sql.jdbc
         |    options (
         |    driver '$driver',
         |    url '$jdbcConnURL',
         |    dbtable '$schemaName.$tableName',
         |    user '$user',
         |    password '$password'
         |    )""".stripMargin
    println(s"createCmd: \n $createCmd")
    sparkSession.sql(createCmd)
    val querySql = "select * from " + sparkTableName + " limit 1"
    sparkSession.sql(querySql).show


    //使用dataset API接口
    val connectionProperties = new Properties()
    connectionProperties.put("driver", driver)
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    //读取数据
    var jdbcDf = sparkSession.read.jdbc(jdbcConnURL,
      s"$database.$schemaName.$tableName",
      connectionProperties)
    jdbcDf.select("name", "age", "score").show()

    val data =
      Seq(
        Person("bill", 30, 170.5D),
        Person("gate", 29, 200.3D)
      )
    val dfWrite = sparkSession.createDataFrame(data)

    //写入数据
    dfWrite
      .write
      .mode("append")
      .jdbc(jdbcConnURL, s"$database.$schemaName.$tableName", connectionProperties)
    jdbcDf.select("name", "age").show()
    sparkSession.stop()
  }

}