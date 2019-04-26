package com.aliyun.spark.polardb

import java.sql.DriverManager
import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Date, Properties}

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SparkSession

/**
  * Spark归档PolarDB增量数据示例, 前提：PolarDB表中带有增量时间
  * 以下表作为示例，其中
  * 测试表结构：
  *    CREATE TABLE t_cc_user_stat(
  *                      id int,
  *                      cc_id varchar(36),
  *                      user_id varchar(36),
  *                      join_hand_up_count int,
  *                      hand_up_count int,
  *                      join_vies_answer_count int,
  *                      vies_answer_count int,
  *                      create_time datetime,
  *                      last_update_time datetime)
  * 初始数据：
  *    INSERT INTO t_cc_user_stat VALUES(1, 'cc_1234','user11',5,1,3,2,'2019-04-13', '2019-04-13');
  *    INSERT INTO t_cc_user_stat VALUES(2, 'cc_1234','user12',43,15,45,6,'2019-04-13', '2019-04-13');
  *    INSERT INTO t_cc_user_stat VALUES(3, 'cc_1234','user13',23,11,13,8,'2019-04-13', '2019-04-13');
  *    INSERT INTO t_cc_user_stat VALUES(4, 'cc_1234','user14',15,7,13,9,'2019-04-13', '2019-04-14');
  *    INSERT INTO t_cc_user_stat VALUES(5, 'cc_1234','user15',15,5,4,1,'2019-04-14', '2019-04-14');
  *    INSERT INTO t_cc_user_stat VALUES(6, 'cc_1235','user16',9,1,1,1,'2019-04-13', '2019-04-13');
  *    INSERT INTO t_cc_user_stat VALUES(7, 'cc_1235','user17',4,0,2,1,'2019-04-13', '2019-04-13');
  *    INSERT INTO t_cc_user_stat VALUES(8, 'cc_1235','user18',0,0,0,0,'2019-04-13', '2019-04-13');
  *    INSERT INTO t_cc_user_stat VALUES(9, 'cc_1235','user19',1,1,4,2,'2019-04-13', '2019-04-13');
  *    INSERT INTO t_cc_user_stat VALUES(10, 'cc_1236','user20',44,11,23,2,'2019-04-13', '2019-04-13');
  *    INSERT INTO t_cc_user_stat VALUES(11, 'cc_1236','user21',76,33,31,22,'2019-04-13', '2019-04-13');
  * 数据更新：
  *    UPDATE t_cc_user_stat SET join_hand_up_count=33 AND hand_up_count=23 AND last_update_time=CURRENT_DATE WHERE user_id='user14'
  *    UPDATE t_cc_user_stat SET join_hand_up_count=95 AND hand_up_count=55 AND last_update_time=CURRENT_DATE WHERE user_id='user16'
  *    UPDATE t_cc_user_stat SET join_hand_up_count=100 AND hand_up_count=11 AND last_update_time=CURRENT_DATE WHERE user_id='user21'
  *
  * 增量归档流程：
  * 1. 归档PolarDB初始全量数据，可参考 SparkOnPolarDBArchive
  * 2. PolarDB数据更新后，通过更新时间过滤出更新数据
  * 3. 全量数据与更新数据进行join，关联出更新后数据，创建临时表
  * 4. 使用已归档全量表与更新数据join出未更新数据，与更新数据进行Union
  * 5. 数据覆盖写入全量表
  *
  * 说明：demo为体现完整性，带有polardb建表、写数据和更新操作，实际场景中针对已存在的表，
  *      动态传入更新时间过滤即可
  */
object SparkOnPolarDBIncrement {

  lazy val CREATE_TABLE_SQL = s"CREATE TABLE IF NOT EXISTS table_name(" +
    "id int, cc_id varchar(36), user_id varchar(36), join_hand_up_count int, " +
    "hand_up_count int,join_vies_answer_count int, vies_answer_count int, " +
    "create_time datetime, last_update_time datetime)"
  lazy val INSERT_SQL_ARRAY = Array(
    "INSERT INTO table_name VALUES(1, 'cc_1234','user11',5,1,3,2,'2019-04-13', '2019-04-13')",
    "INSERT INTO table_name VALUES(2, 'cc_1234','user12',43,15,45,6,'2019-04-13', '2019-04-13')",
    "INSERT INTO table_name VALUES(3, 'cc_1234','user13',23,11,13,8,'2019-04-13', '2019-04-13')",
    "INSERT INTO table_name VALUES(4, 'cc_1234','user14',15,7,13,9,'2019-04-13', '2019-04-14')",
    "INSERT INTO table_name VALUES(5, 'cc_1234','user15',15,5,4,1,'2019-04-14', '2019-04-14')",
    "INSERT INTO table_name VALUES(6, 'cc_1235','user16',9,1,1,1,'2019-04-13', '2019-04-13')",
    "INSERT INTO table_name VALUES(7, 'cc_1235','user17',4,0,2,1,'2019-04-13', '2019-04-13')",
    "INSERT INTO table_name VALUES(8, 'cc_1235','user18',0,0,0,0,'2019-04-13', '2019-04-13')",
    "INSERT INTO table_name VALUES(9, 'cc_1235','user19',1,1,4,2,'2019-04-13', '2019-04-13')",
    "INSERT INTO table_name VALUES(10, 'cc_1236','user20',44,11,23,2,'2019-04-13', '2019-04-13')",
    "INSERT INTO table_name VALUES(11, 'cc_1236','user21',76,33,31,22,'2019-04-13', '2019-04-13')"
  )
  lazy val UPDATE_SQL_ARRAY = Array(
    "UPDATE table_name SET join_hand_up_count=33, hand_up_count=23, last_update_time=CURRENT_DATE WHERE user_id='user14'",
    "UPDATE table_name SET join_hand_up_count=95, hand_up_count=55, last_update_time=CURRENT_DATE WHERE user_id='user16'",
    "UPDATE table_name SET join_hand_up_count=100, hand_up_count=11, last_update_time=CURRENT_DATE WHERE user_id='user21'",
    "INSERT INTO table_name VALUES(12, 'cc_1236','user23',45,6,18,9,CURRENT_DATE, CURRENT_DATE)"
  )

  val LOG = LogFactory.getLog(SparkOnPolarDBIncrement.getClass)

  def main(args: Array[String]): Unit = {
    //获取POLARDB的 url、tableName、登录POLARDB数据库的user和password
    val url = args(0)
    val jdbcConnURL = s"jdbc:mysql://$url"
    //传入不存在PolarDB表名
    val tableName = args(1)
    val user = args(2)
    val password = args(3)

    //Spark侧的表名。
    var sparkTableName = if (args.size > 4) args(4) else "spark_on_polardb"

    //STEP1: 创建PolarDB表并写数据(实际场景忽略此步骤，传入已存在表名即可)
    initPolarDBTable(tableName, jdbcConnURL, user, password)

    //STEP2：创建Spark polar映射表，并全量归档表数据
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("Spark PolarDB Archive")
      .getOrCreate()

    // spark关联PolarDB临时表名
    val spark_tmp_table = "spark_tmp_table"
    archiveHistory(spark, sparkTableName, tableName, jdbcConnURL, user, password, spark_tmp_table)
    //STEP3：更新PolarDB数据
    updatePolarDBTable(tableName, jdbcConnURL, user, password)
    //STEP4：增量归档当前日期数据
    incrementArchive(spark, sparkTableName, tableName, jdbcConnURL, user, password, spark_tmp_table)

  }
  def incrementArchive(spark: SparkSession, sparkTableName: String, polarBDTable: String, jdbcConnURL: String,
                     user: String, password: String, spark_tmp_table: String) = {
    val currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val where = s"last_update_time = '$currentDate'"
    //
    var selectCols = ""
    //读取PolarDB表数据
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", user)
    properties.put("password", password)
    val polarDBDataFrame = spark.read.jdbc(jdbcConnURL, polarBDTable, properties).filter(where)

    polarDBDataFrame.schema.fields.foreach(field => {
      selectCols = selectCols.concat(field.name + ",")
    })
    selectCols = selectCols.substring(0, selectCols.length - 1)
    polarDBDataFrame.createOrReplaceTempView(spark_tmp_table)
    val createTmpSparkSQL = s"CREATE TABLE  tmp_$sparkTableName as " +
      s"select $selectCols from $spark_tmp_table " +
      s"union " +
      s"select $sparkTableName.* from $sparkTableName " +
      s"left outer join $spark_tmp_table on $sparkTableName.user_id=$spark_tmp_table.user_id " +
      s"where $spark_tmp_table.user_id is null"
    LOG.info(createTmpSparkSQL)
    spark.sql(createTmpSparkSQL)
    val overwriteSparkSQL = s"insert overwrite table $sparkTableName select * from tmp_$sparkTableName"
    spark.sql(overwriteSparkSQL)
    val dropTmpTableSQL= s"drop table tmp_$sparkTableName"
    spark.sql(dropTmpTableSQL)
  }
    def archiveHistory(spark: SparkSession, sparkTableName: String, polarBDTable: String, jdbcConnURL: String,
                     user: String, password: String, spark_tmp_table: String) = {
    var createArchiveTableCmd = "create table if not exists " + sparkTableName + "("

    var selectCols = ""
    //读取PolarDB表数据
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", user)
    properties.put("password", password)
    val polarDBDataFrame = spark.read.jdbc(jdbcConnURL, polarBDTable, properties)

    polarDBDataFrame.schema.fields.foreach(field => {
      selectCols = selectCols.concat(field.name + ",")
      createArchiveTableCmd = createArchiveTableCmd.concat(field.name + " " + field.dataType.typeName + ",")
    })
    selectCols = selectCols.substring(0, selectCols.length - 1)
    createArchiveTableCmd = createArchiveTableCmd.substring(0, createArchiveTableCmd.length - 1) + ")" + "STORED AS parquet "

    spark.sql(createArchiveTableCmd)

    polarDBDataFrame.createOrReplaceTempView(spark_tmp_table)
    // 抽取polardb数据并归档至spark表
    val extractSql = "INSERT INTO " + sparkTableName + " SELECT " + selectCols + " from " + spark_tmp_table
    spark.sql(extractSql)
  }

  def initPolarDBTable(tableName: String, url: String, user: String, passwd: String): Unit = {
    val conn = DriverManager.getConnection(url, user, passwd)
    val stat = conn.createStatement()
    stat.execute(CREATE_TABLE_SQL.replace("table_name", tableName))
    INSERT_SQL_ARRAY.foreach(insertSQL => stat.execute(insertSQL.replace("table_name", tableName)))
    stat.close()
    conn.close()
  }

  def updatePolarDBTable(tableName: String, url: String, user: String, passwd: String): Unit = {
    val conn = DriverManager.getConnection(url, user, passwd)
    val stat = conn.createStatement()
    UPDATE_SQL_ARRAY.foreach(updateSQL => stat.execute(updateSQL.replace("table_name", tableName)))
    stat.close()
    conn.close()
  }

}
