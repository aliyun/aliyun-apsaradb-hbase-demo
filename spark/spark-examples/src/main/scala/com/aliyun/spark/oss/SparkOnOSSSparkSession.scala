package com.aliyun.spark.oss

import java.io.ByteArrayInputStream

import com.aliyun.oss.{OSSClient, OSSClientBuilder}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkOnOSSSparkSession {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark sql test")
    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val bucketName = args(0)
    val securityId = args(1)
    val securityKey = args(2)
    val endpoint = args(3)
    var sparkTable = args(4)

    var sql = s"drop table if exists ${sparkTable}"
    sparkSession.sql(s"drop table if exists ${sparkTable}")

    //创建spark表，表数据路径在OSS上。
    sql =
      s"""create table ${sparkTable}(
         |      c_date          date,
         |      c_timestamp     timestamp,
         |      c_int           int,
         |      c_double        double,
         |      c_float         float,
         |      c_string        string
         |      ) using parquet
         |      partitioned by(c_date)
         |      location 'oss://$securityId:$securityKey@$bucketName.$endpoint/user/spark-table/house/default/$sparkTable'
         |      """.stripMargin
    println(s"create cmd: \n $sql")
    sparkSession.sql(sql)
    //向表中插入数据
    for (i <- 1 to 10) {
      val insertSql = s"insert into ${sparkTable} values" +
        s"(${System.currentTimeMillis()},$i,4.04,5.5,'string-value-$i','2019-01-0${i % 3 + 1}')"
      println(insertSql)
      sparkSession.sql(insertSql)
    }
    //查询数据
    sql = s"select * from ${sparkTable}"
    var result = sparkSession.sql(sql)
    println(s"the ${sparkTable} count = ${result.count()}")
    result.show()

    val resultTableName = s"${sparkTable}_result"

    sparkSession.sql(s"drop table if exists ${resultTableName}")

    sql =
      s"""create table $resultTableName(
         |      c_date          date,
         |      c_timestamp     timestamp,
         |      c_int           int,
         |      c_double        double,
         |      c_float         float,
         |      c_string        string
         |      ) using parquet
         |      partitioned by(c_date)
         |      location 'oss://$securityId:$securityKey@$bucketName.$endpoint/user/spark-table/house/default/$resultTableName'
         |      """.stripMargin
    println(s"create cmd: \n $sql")

    sparkSession.sql(sql)

    val insertCmd = s"insert into $resultTableName select * from $sparkTable"
    println(s"=insertCmd=$insertCmd")
    sparkSession.sql(insertCmd)
    sql = s"select * from ${resultTableName}"
    result = sparkSession.sql(sql)
    println(s"==============插入和查询oss路径表================")
    result.show()
    println(s"==============插入和查询oss路径表================")
    println("\n\n")

    //use dataFrame
    sql = s"select c_date, c_int from $resultTableName"
    result = sparkSession.sql(sql)

    //使用DataFrame写数据到OSS
    val ossParquetPath = s"oss://$securityId:$securityKey@$bucketName.$endpoint/user/spark-table/write_parquet/$resultTableName/"
    result.write.parquet(ossParquetPath)

    val readParquetTableName = s"${sparkTable}_read_parquet"
    val parquetDF = sparkSession.read.parquet(ossParquetPath)
    parquetDF.createOrReplaceTempView(readParquetTableName)
    sql = s"select * from $readParquetTableName"
    result = sparkSession.sql(sql)
    println(s"==============DataFrame写oss 路径================")
    result.show()
    println(s"==============DataFrame写oss 路径================")
    println("\n\n")


    //oss 界面上传csv 文件，通过建表和DataFrame读取数据。
    var csvTableName = "csv_table"
    val csvPath = s"oss://$securityId:$securityKey@$bucketName.$endpoint/user_file/csv/"
    sql =
      s"""create table $csvTableName(
         |      c_date          string,
         |      c_timestamp     long,
         |      c_int           int,
         |      c_double        double,
         |      c_float         float,
         |      c_string        string
         |      ) row format delimited fields terminated by ','
         |      location '$csvPath'
         |      """.stripMargin

    sparkSession.sql(s"drop table if exists ${csvTableName}")
    sparkSession.sql(sql)
    result = sparkSession.sql(s"select * from $csvTableName")
    println(s"==============Spark table读oss界面上传的csv数据================")
    result.show()
    println(s"==============Spark table读oss界面上传的csv数据================")
    println("\n\n")

    println(s"==============DataFrame 读oss界面上传的csv 数据================")
    val csvdf = sparkSession.read.csv(s"$csvPath")
    csvdf.printSchema()
    csvdf.show()
    println(s"==============DataFrame 读oss界面上传的csv 数据================")
    println("\n\n")

    //使用OSSclient上传数据到oss，然后使用DataFrame读数据。
    val ossClient = new OSSClientBuilder().build(endpoint, securityId, securityKey)
    val ossKey = "user/file/ossclient_put/file.csv"
    val content =
      s"""
         |'2019-01-01', 1572247318, 1, 2.222,3.3, 'string-value01'
         |'2019-01-02', 1572247318, 1, 2.222,3.3, 'string-value02'
         |'2019-01-03', 1572247318, 1, 2.222,3.3, 'string-value03'
         |'2019-01-04', 1572247318, 1, 2.222,3.3, 'string-value04'
         |'2019-01-05', 1572247318, 1, 2.222,3.3, 'string-value05'
       """.stripMargin
    ossClient.putObject(bucketName, ossKey, new ByteArrayInputStream(content.getBytes))
    println(s"==============DataFrame 读ossClient上传的csv数据================")
    val ossKeyPath = s"oss://$securityId:$securityKey@$bucketName.$endpoint/$ossKey"
    sparkSession.read.csv(ossKeyPath).show()
    println(s"==============DataFrame 读ossClient上传的csv数据================")

    ossClient.shutdown()
    sparkSession.stop()
  }
}
