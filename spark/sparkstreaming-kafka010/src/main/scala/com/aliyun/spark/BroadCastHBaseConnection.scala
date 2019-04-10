package com.aliyun.spark
import java.io.IOException
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

class BroadCastHBaseConnection(createhbaseconnection:() => org.apache.hadoop.hbase.client.Connection) extends Serializable{
  lazy val connection = createhbaseconnection()
  @throws[IOException]
  def getTable(tableName: TableName): Table = connection.getTable(tableName)
}


object BroadCastHBaseConnection {

  def apply[K, V](zkURI:String): BroadCastHBaseConnection = {
    val createHBaseConnectionFunc = () => {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkURI)
      val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
      sys.addShutdownHook({
        hbaseConnection.close()
      })
      hbaseConnection
    }
    new BroadCastHBaseConnection(createHBaseConnectionFunc)
  }

}