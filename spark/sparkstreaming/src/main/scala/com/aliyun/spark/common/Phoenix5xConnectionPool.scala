package com.aliyun.spark.common

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}

object Phoenix5xConnectionPool {

  private var connectionPool:GenericObjectPool[Connection] = null

  def apply(queryServerAddress: String, properties: Properties = null): GenericObjectPool[Connection] = {
    if (connectionPool == null) {
      connectionPool = new GenericObjectPool[Connection](new PhoenixConnectionFactory(queryServerAddress, properties))
    }
    connectionPool
  }
}


class PhoenixConnectionFactory(queryServerAddress: String, properties: Properties = null) extends BasePooledObjectFactory[Connection] {

  /**
    * 轻客户端URL驱动类名
    */
  private val DRIVER_CLASS_STRING = "org.apache.phoenix.queryserver.client.Driver"
  /**
    * 轻客户端URL前缀
    */
  private val CONNECT_STRING_PREFIX = "jdbc:phoenix:thin:"
  /**
    * 缺省序列化协议
    */
  private val DEFAULT_SERIALIZATION = "PROTOBUF"

  override def create(): Connection = {
    val url = getConnectionUrl(queryServerAddress, DEFAULT_SERIALIZATION)
    Class.forName(DRIVER_CLASS_STRING)
    if (properties != null) {
      DriverManager.getConnection(url, properties)
    } else {
      DriverManager.getConnection(url)
    }
  }

  override def wrap(connection: Connection): PooledObject[Connection] = {
    new DefaultPooledObject[Connection](connection)
  }

  override def destroyObject(p: PooledObject[Connection]): Unit = {
    p.getObject.close()
  }

  override def validateObject(p: PooledObject[Connection]): Boolean = {
    !p.getObject.isClosed
  }

  def getConnectionUrl(queryServerAddress: String, serialization: String): String = {
    val urlFmt = CONNECT_STRING_PREFIX + "url=%s;serialization=%s"
    String.format(urlFmt, queryServerAddress, serialization)
  }
}