package com.aliyun.spark

import java.sql.{DriverManager, SQLException}

/**
  * 运行spark on HBase/Phoenix Scala JDBC demo 依赖的jar包,需要在java版本依赖的基础上再加上scala的jar包。
  */

object SparkOnHBaseJDBC {
  def main(args: Array[String]): Unit = {
    // Spark JDBC Driver 路径。
    val driver = "org.apache.hive.jdbc.HiveDriver"
    // ThriftServer访问地址，可以从Spark集群详情获取。使用时请把此路径替换为你自己的Spark集群的ThriftServer访问地址。
    // 格式为：jdbc:hive2://xxx-001.spark.9b78df04-b.rds.aliyuncs.com:10000;
    val thriftServerAdress = args(0)
    //Spark侧的表名。
    val sparkTableName = "spark_hbase"
    //hbase侧的表名，需要在hbase侧提前创建。hbase表创建可以参考：https://help.aliyun.com/document_detail/52051.html?spm=a2c4g.11174283.6.577.7e943c2eiYCq4k
    val phoenixTableName = "mytable"
    //HBase集群的ZK链接地址。//HBase集群的ZK链接地址。使用时请把此路径替换为你自己的HBase集群的zk访问地址。
    //格式为：xxx-002.hbase.rds.aliyuncs.com:2181,xxx-001.hbase.rds.aliyuncs.com:2181,xxx-003.hbase.rds.aliyuncs.com:2181
    val zkAddress = args(1)
    try {
      Class.forName(driver)
    } catch {
      case e: ClassNotFoundException => e.printStackTrace
    }
    val conn = DriverManager.getConnection(thriftServerAdress)
    try {
      val stmt = conn.createStatement
      //建表语句
      val createCmd = s"""CREATE TABLE ${sparkTableName} USING org.apache.hadoop.hbase.spark
                        |    OPTIONS ('catalog'=
                        |    '{"table":{"namespace":"default", "name":"${phoenixTableName}"},"rowkey":"rowkey1",
                        |    "columns":{
                        |    "col0":{"cf":"rowkey", "col":"rowkey1", "type":"string"},
                        |    "col1":{"cf":"cf", "col":"col1", "type":"String"}}}',
                        |    'hbase.zookeeper.quorum' = '${zkAddress}'
                        |    )""".stripMargin

      println(" createCmd: \n" + createCmd)
      //创建表
      stmt.execute(createCmd)
      val querySql = "select * from " + sparkTableName + " limit 1"
      val pstmt = conn.prepareStatement(querySql)
      val resultSet = pstmt.executeQuery
      //打印查询结果
      if (resultSet.next()) {
        println(resultSet.getString(1) + " | " +
          resultSet.getString(2))
      }
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      if (null != conn) {
        conn.close()
      }
    }
  }
}
