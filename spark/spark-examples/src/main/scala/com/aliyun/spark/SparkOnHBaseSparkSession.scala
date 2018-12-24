package com.aliyun.spark

import org.apache.spark.sql.SparkSession

/**
  * 运行命令
  * spark-submit --master yarn --class com.aliyun.spark.SparkOnHBaseSparkSession /opt/spark-examples-0.0.1-SNAPSHOT.jar zkAddress
  */
object SparkOnHBaseSparkSession {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport() //使用hive-metastore后通过beenline可以查看到代码中创建的表
      .appName("scala spark on HBase test")
      .getOrCreate()

    // Spark JDBC Driver 路径。
    //Spark侧的表名。
    val sparkTableName = "spark_hbase"
    //hbase侧的表名，需要在hbase侧提前创建。hbase表创建可以参考：https://help.aliyun.com/document_detail/52051.html?spm=a2c4g.11174283.6.577.7e943c2eiYCq4k
    val hbaseTableName = "mytable"
    //HBase集群的ZK链接地址。//HBase集群的ZK链接地址。使用时请把此路径替换为你自己的HBase集群的zk访问地址。
    //格式为：xxx-002.hbase.rds.aliyuncs.com:2181,xxx-001.hbase.rds.aliyuncs.com:2181,xxx-003.hbase.rds.aliyuncs.com:2181
    val zkAddress = args(0)
    val createCmd = s"""CREATE TABLE ${sparkTableName} USING org.apache.hadoop.hbase.spark
                       |    OPTIONS ('catalog'=
                       |    '{"table":{"namespace":"default", "name":"${hbaseTableName}"},"rowkey":"rowkey1",
                       |    "columns":{
                       |    "col0":{"cf":"rowkey", "col":"rowkey1", "type":"string"},
                       |    "col1":{"cf":"cf", "col":"col1", "type":"String"}}}',
                       |    'hbase.zookeeper.quorum' = '${zkAddress}'
                       |    )""".stripMargin

    println(" createCmd: \n" + createCmd)
    sparkSession.sql(createCmd)
    val querySql = "select * from " + sparkTableName + " limit 1"
    sparkSession.sql(querySql).show
    sparkSession.stop()
  }
}
