package com.aliyun.spark.sink

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.{ForeachWriter, Row}

class PostgreSqlSink(url: String, user: String, pw: String, table: String) extends ForeachWriter[Row]{

  var conn: Connection = _

  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName("org.postgresql.Driver")
    conn = DriverManager.getConnection(url, user, pw)
    true
  }

  override def process(value: Row): Unit = {
    val pstm = conn.prepareStatement(s"insert into $table values(?, ?, ?)")
    pstm.setString(1, value.getString(0))
    pstm.setString(2, value.getString(1))
    pstm.setLong(3, value.getLong(2))
    pstm.execute()
  }

  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}
