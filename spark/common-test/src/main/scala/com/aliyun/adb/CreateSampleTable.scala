package com.aliyun.adb

import java.sql.DriverManager

object CreateSampleTable {

  def main(args: Array[String]): Unit = {
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    val url = args(0)
    val user = args(1)
    val password = args(2)
    val tableName = args(3)

    println(s"url=$url")
    println(s"user=$user")
    println(s"password=$password")

    val conn = DriverManager.getConnection(url, user, password)

    val stmt = conn.createStatement()
    val sql =
      s"""
         | CREATE TABLE postgres.public.$tableName (
         | name varchar(32) NULL,
         | age int  NULL,
         | score double precision  NULL
         |)
       """.stripMargin
    stmt.execute(sql)

    for(i <- 1 to 5) {
      stmt.execute(s"INSERT INTO postgres.public.$tableName " +
        s"values('aliyun0${i}', ${100 + i}, ${10 + i/10})")
    }

    val result = stmt.executeQuery(s"select * from postgres.public.$tableName")
    while(result.next()) {
      print(result.getString(1))
      print("   ")
      print(result.getInt(2))
      print("   ")
      println(result.getDouble(3))
    }
  }


}
