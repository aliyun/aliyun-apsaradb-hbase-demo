package com.aliyun.spark.polardb

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkOnPOLARDBSparkSession {

  def main(args: Array[String]): Unit = {
    //获取POLARDB的 url、database、tableName、登录POLARDB数据库的user和password
    val url = args(0)
    val jdbcConnURL = s"jdbc:mysql://$url"
    val database = args(1)
    val tableName = args(2)
    val user = args(3)
    val password = args(4)

    //Spark侧的表名。
    var sparkTableName = if (args.size > 5) args(5) else "spark_on_polardb_sparksession_test01"

    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport() //可选，使用hive-metastore后通过thriftServer可以查看到代码中创建的表
      .appName("scala spark on POLARDB test")
      .getOrCreate()

    //Sql方式，Spark会映射POLARDB中表的Schema。
    sparkTableName = sparkTableName + "_noschema"
    var createCmd =
      s"""CREATE TABLE ${sparkTableName} USING org.apache.spark.sql.jdbc
         |    options (
         |    url '$jdbcConnURL',
         |    dbtable '$database.$tableName',
         |    user '$user',
         |    password '$password'
         |    )""".stripMargin

    sparkSession.sql(createCmd)
    var querySql = "select * from " + sparkTableName + " limit 1"
    sparkSession.sql(querySql).show


    //使用dataset API接口
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    //读取数据
    var jdbcDf = sparkSession.read.jdbc(jdbcConnURL, s"$database.$tableName", connectionProperties)
    jdbcDf.select("name", "age", "score").show()

    val data =
      Seq(
        Person("bill", 30, 170.5),
        Person("gate", 29, 200.3)
      )
    val dfWrite = sparkSession.createDataFrame(data)

    //写入数据
    dfWrite
      .write
      .mode("append")
      .jdbc(jdbcConnURL, s"$database.$tableName", connectionProperties)
    jdbcDf.select("name", "age").show()
    sparkSession.stop()
  }

}

case class Person(name: String, age: Int, score: Double)