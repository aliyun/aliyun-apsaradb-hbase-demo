package com.aliyun.spark.mongodb

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.bson.Document

object SparkOnMongoDBSparkSession {

  def main(args: Array[String]): Unit = {
    //获取MongoDB的 connectionStringURI，database 和 collection
    val connectionStringURI = args(0)
    val database = args(1)
    val collection = args(2)
    //Spark侧的表名。
    var sparkTableName = if (args.size > 3) args(3) else "spark_on_mongodb_sparksession_test01"

    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport() //可选，使用hive-metastore后通过thriftServer可以查看到代码中创建的表
      .appName("scala spark on MongoDB test")
      .getOrCreate()

    //Spark 读取MongoDB数据有多种方式。
    //使用Dataset API方式：
    //设置MongoDB的参数
    val sparkConf = new SparkConf()
      .set("spark.mongodb.input.uri", connectionStringURI)
      .set("spark.mongodb.input.database", database)
      .set("spark.mongodb.input.collection", collection)
      .set("spark.mongodb.output.uri", connectionStringURI)
      .set("spark.mongodb.output.database", database)
      .set("spark.mongodb.output.collection", collection)
    val readConf = ReadConfig(sparkConf)
    //获取Dataframe
    val df = MongoSpark.load(sparkSession, readConf)
    df.show(1)

    //使用MongoSpark.save入库数据到MongoDB
    val docs =
      """
        |{"id": "id105", "name": "name105"}
        |{"id": "id106", "name": "name106"}
        |{"id": "id107", "name": "name107"}
        |"""
        .trim.stripMargin.split("[\\r\\n]+").toSeq
    val writeConfig: WriteConfig = WriteConfig(Map(
      "uri" -> connectionStringURI,
      "spark.mongodb.output.database" -> database,
      "spark.mongodb.output.collection"-> collection))
    MongoSpark.save(sparkSession.sparkContext.parallelize(docs.map(Document.parse)), writeConfig)

    //使用Sql的方式，SQL的方式有两种，指定Schema和不指定Schema
    //指定Schema的创建方式，Schema中的字段必须和MongoDB中Collection的Schema一致。
    var createCmd =
    s"""CREATE TABLE ${sparkTableName} (
       |      id String,
       |      name String
       |    ) USING com.mongodb.spark.sql
       |    options (
       |    uri '$connectionStringURI',
       |    database '$database',
       |    collection '$collection'
       |    )""".stripMargin

    sparkSession.sql(createCmd)
    var querySql = "select * from " + sparkTableName + " limit 1"
    sparkSession.sql(querySql).show

    //不指定Schema的创建方式，不指定Schema，Spark会映射MOngoDB中collection的Schema。
    sparkTableName = sparkTableName + "_noschema"
    createCmd =
      s"""CREATE TABLE ${sparkTableName} USING com.mongodb.spark.sql
         |    options (
         |    uri '$connectionStringURI',
         |    database '$database',
         |    collection '$collection'
         |    )""".stripMargin

    sparkSession.sql(createCmd)
    querySql = "select * from " + sparkTableName + " limit 1"
    sparkSession.sql(querySql).show

    sparkSession.stop()

  }
}
