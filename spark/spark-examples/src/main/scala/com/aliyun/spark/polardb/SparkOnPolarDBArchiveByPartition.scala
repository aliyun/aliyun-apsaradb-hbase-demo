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
    //增量归档数据条件，如只归档某段时间数据
    val where = args(4)
    //分区字段名，必须在polardb表中存在
    val partitionKey = args(5)

    //Spark侧的表名。
    var sparkTableName = if (args.size > 6) args(6) else "spark_on_polardb"

    val spark = SparkSession
      .builder()
      .enableHiveSupport() //可选，使用hive-metastore后通过thriftServer可以查看到代码中创建的表
      .appName("Spark PolarDB Archive")
      .getOrCreate()

    //创建Spark归档表，存储格式为parquet，按系统日期自动分区分区
    var createArchiveTableCmd = "create table if not exists " + sparkTableName + "("

    var partitionCmd = "PARTITIONED BY("
    var selectCols = ""
    //读取PolarDB表数据
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", user)
    properties.put("password", password)
    val polarDBDataFrame = spark.read.jdbc(jdbcConnURL, tableName, properties)

    polarDBDataFrame.schema.fields.foreach(field =>
      if(field.name.equalsIgnoreCase(partitionKey)) {
        partitionCmd = partitionCmd.concat(field.name + " " + field.dataType.typeName + ")")
      } else {
        selectCols = selectCols.concat(field.name + ",")
        createArchiveTableCmd = createArchiveTableCmd.concat(field.name + " " + field.dataType.typeName + ",")
      } )
    createArchiveTableCmd = createArchiveTableCmd.substring(0, createArchiveTableCmd.length - 1) + ")" +
      "STORED AS parquet " + partitionCmd

    spark.sql(createArchiveTableCmd)
    //开启动态分区
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    polarDBDataFrame.createOrReplaceTempView("spark_tmp_view")
    // 抽取polardb数据并归档至spark表，增量归档数据根据where条件指定
    val extractSql = "INSERT INTO " + sparkTableName +
      " partition(" + partitionKey + ") SELECT " + selectCols + partitionKey + " from spark_tmp_view where " + where
    spark.sql(extractSql)
    spark.close()
  }

}
