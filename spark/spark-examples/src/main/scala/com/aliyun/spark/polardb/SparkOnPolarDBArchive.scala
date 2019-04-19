package com.aliyun.spark.polardb

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkOnPolarDBArchive {

  def main(args: Array[String]): Unit = {
    //获取POLARDB的 url、tableName、登录POLARDB数据库的user和password
    val url = args(0)
    val jdbcConnURL = s"jdbc:mysql://$url"
    val tableName = args(1)
    val user = args(2)
    val password = args(3)
    //Spark侧的表名。
    var sparkTableName = if (args.size > 4) args(4) else "spark_on_polardb"

    val spark = SparkSession
      .builder()
      .enableHiveSupport() //可选，使用hive-metastore后通过thriftServer可以查看到代码中创建的表
      .appName("Spark PolarDB Archive")
      .getOrCreate()

    //创建Spark归档表，存储格式为parquet，按系统日期自动分区分区
    var createArchiveTableCmd = "create table if not exists " + sparkTableName + "("
    //读取PolarDB表数据
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", user)
    properties.put("password", password)
    val polarDBDataFrame = spark.read.jdbc(jdbcConnURL, tableName, properties)

    polarDBDataFrame.schema.fields.foreach(field =>
      createArchiveTableCmd = createArchiveTableCmd.concat(field.name + " " + field.dataType.typeName + ","))

    createArchiveTableCmd = createArchiveTableCmd.substring(0, createArchiveTableCmd.length - 1) + ")" +
      "STORED AS parquet PARTITIONED BY(daytime date)"

    spark.sql(createArchiveTableCmd)
    //开启动态分区
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    polarDBDataFrame.createOrReplaceTempView("spark_tmp_view")
    // 抽取polardb数据并归档至spark表，以当前日期进行分区
    val extractSql = "INSERT INTO " + sparkTableName +
      " partition(daytime) SELECT *, from_unixtime(unix_timestamp(), 'yyyy-MM-dd') from spark_tmp_view"
    spark.sql(extractSql)

    spark.close()
  }
}
