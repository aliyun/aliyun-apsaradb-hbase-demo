package com.aliyun.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Random

object SparkSqlSparkSession {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark sql test")
    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val baseTableName = "spark_table_test"

    var sparkTable = baseTableName + "_01"

    println(s"====================${sparkTable}=====================")
    //hive table
    var sql = s"drop table if exists ${sparkTable}"
    sparkSession.sql(sql)
    sql = s"""create table ${sparkTable}(
            |      ss_sold_date_sk       int,
            |      ss_sold_time_sk       int,
            |      ss_item_sk            int,
            |      ss_customer_sk        int,
            |      ss_cdemo_sk           int,
            |      ss_hdemo_sk           int,
            |      ss_addr_sk            int,
            |      ss_store_sk           int,
            |      ss_promo_sk           int,
            |      ss_ticket_number      long,
            |      ss_quantity           int,
            |      ss_wholesale_cost     decimal(7,2),
            |      ss_list_price         decimal(7,2),
            |      ss_sales_price        decimal(7,2),
            |      ss_ext_discount_amt   decimal(7,2),
            |      ss_ext_sales_price    decimal(7,2),
            |      ss_ext_wholesale_cost decimal(7,2),
            |      ss_ext_list_price     decimal(7,2),
            |      ss_ext_tax            decimal(7,2),
            |      ss_coupon_amt         decimal(7,2),
            |      ss_net_paid           decimal(7,2),
            |      ss_net_paid_inc_tax   decimal(7,2),
            |      ss_net_profit         decimal(7,2))
            |      row format delimited fields terminated by '|'
            |      """.stripMargin
    sparkSession.sql(sql)
    sql = s"insert into ${sparkTable} values(1,2,3,4,5,6,7,8,9,10,11,1.1,2.1,3.1,4.1,5.1,6.1,7.1,8.1,9.1,10.1,11.1,12.1)"
    sparkSession.sql(sql)
    sql = s"select * from ${sparkTable}"
    var result = sparkSession.sql(sql)
    result.show()
    sql = s"select count(*) from ${sparkTable}"
    result = sparkSession.sql(sql)
    println(s"the ${sparkTable} count = ${result.count()}")
    println(s"====================${sparkTable}=====================")

    //parquet table
    sparkTable = baseTableName + "_02"
    println(s"====================${sparkTable}=====================")
    sql = s"drop table if exists ${sparkTable}"
    sparkSession.sql(sql)
    sql = s"""create table ${sparkTable}(
             |	id int,
             |	name string,
             |	value double
             |) using parquet""".stripMargin
    sparkSession.sql(sql)
    for(i <- 1 to 10) {
      sparkSession.sql(s"insert into ${sparkTable} values(${1000 + i}, 'name${100 + i}', 100.${i})")
    }
    sql = s"select * from ${sparkTable} where id = 1003"
    result = sparkSession.sql(sql)
    result.show()
    println(s"the ${sparkTable} count = ${result.count()}")
    println(s"====================${sparkTable}=====================")

    //parquet partition table
    sparkTable = baseTableName + "_03"
    println(s"====================${sparkTable}=====================")
    sql = s"drop table if exists ${sparkTable}"
    sparkSession.sql(sql)
    sql = s"""create table ${sparkTable}(
             |	id int,
             |	name string,
             |	value double,
             |  dt string
             |) using parquet
             |  partitioned by(dt)
             |  """.stripMargin
    sparkSession.sql(sql)
    for(i <- 1 to 10) {
      sparkSession.sql(s"insert into ${sparkTable} values(${1000 + i},'name${100 + i}',100.${i}," +
        s"'2019-01-0${new Random().nextInt(5)}')")
    }
    sparkSession.sql(s"insert into ${sparkTable} values(1010,'name_1010',100.2," +
      s"'2019-01-02')")

    sql = s"select * from ${sparkTable}"
    sparkSession.sql(sql).show()
    sql = s"select * from ${sparkTable} where dt = '2019-01-02'"
    result = sparkSession.sql(sql)
    println(s"the ${sparkTable} count = ${result.count()}")
    println(s"====================${sparkTable}=====================")

    sparkSession.stop()

  }

}
