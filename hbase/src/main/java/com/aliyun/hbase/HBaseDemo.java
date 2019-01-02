package com.aliyun.hbase;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDemo {

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();

    //为connection设置HBase连接串
    conf.set(HConstants.ZOOKEEPER_QUORUM, "zk1, zk2, zk3");
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");

    //Connection为线程安全对象，在整个程序的生命周期里，只需构造一个Connection对象
    //在程序结束后，需要将Connection对象关闭，否则会造成连接泄露。可以采用try finally方式防止泄露
    Connection connection = ConnectionFactory.createConnection(conf);
    try {
      Admin admin = connection.getAdmin();

      //在HBase中创建表
      createTable(admin, "tablename", Bytes.toBytes("familyname"));

      //Table 为非线程安全对象，每个线程在对Table操作时，都必须从Connection中获取相应的Table对象
      Table table = connection.getTable(TableName.valueOf("tablename"));

      //在HBase中插入一行数据，如果需要插入多行，可以调用table.put(List<Put> puts)接口
      putData(table, Bytes.toBytes("row"), Bytes.toBytes("family"), Bytes.toBytes("column1"),
          Bytes.toBytes("value1"), Bytes.toBytes("column2"), Bytes.toBytes("value2"));

      //在HBase中删除一行数据
      deleteData(table, Bytes.toBytes("row"));

      //在HBase中获取单行数据
      Result result = getData(table, Bytes.toBytes("row"));

      //在HBase中范围扫描数据
      scanData(table, Bytes.toBytes("startKey"), Bytes.toBytes("endKey"), 10);

      //Disable Table
      disableTable(admin, "tablename");

      //删除Table
      deleteTable(admin, "tablename");

      //使用完Table和Admin后，close
      table.close();
      admin.close();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  /**
   * 插入/更新一行的两个列
   * @param row rowKey
   * @param family family名字
   * @param column1 列1的名字
   * @param value1 列1的值
   * @param column2 列2的名字
   * @param value2 列2的值
   * @throws IOException
   */
  public static void putData(Table table, byte[] row, byte[] family,
      byte[] column1, byte[] value1, byte[] column2, byte[] value2) throws IOException {
    Put put = new Put(row);
    put.addColumn(family, column1, value1);
    put.addColumn(family, column2, value2);
    table.put(put);
  }

  /**
   * 删除一行数据
   * @param row
   * @throws IOException
   */
  public static void deleteData(Table table, byte[] row) throws IOException {
    // 删除一整行数据
    Delete delete = new Delete(row);
    table.delete(delete);
    /**
     * 删除一行中的某些列 Delete delete = new Delete(row); delete.deleteColumn(family,
     * column1); delete.deleteColumn(family, column2) table.delete(delete);
     */
  }

  /**
   * 查询一行数据
   * @param table
   * @param row
   * @throws IOException
   */
  public static Result getData(Table table, byte[] row) throws IOException {
    // 查询一整行数据
    Get get = new Get(row);
    Result result = table.get(get);
    return result;
    // 从result中获取某个列的值
    // result.getValue(family, column1);
    /**
     * 查询一行中的某些列 Get get = new Get(row); get.addColumn(family, column1);
     * get.addColumn(family, column2) Result result = table.get(get);
     */
  }

  /**
   * 扫描一个范围的数据
   * @param startRow
   * @param stopRow
   * @param limit
   * @throws IOException
   */
  public static void scanData(Table table, byte[] startRow, byte[] stopRow, int limit)
      throws IOException {
    Scan scan = new Scan(startRow, stopRow);
    //在扫描范围较小时，推荐使用small scanner
    scan.setSmall(true);
    /**
     * 只扫描某些列 scan.addColumn(family, column1); scan.addColumn(family,
     * column2);
     */
    ResultScanner scanner = table.getScanner(scan);
    int readCount = 0;
    for (Result result : scanner) {
      readCount++;
      // 处理查询结果result
      // ...
      if (readCount >= limit) {
        // 查询数量达到limit值，终止扫描
        break;
      }
    }
    // 最后需要关闭scanner
    scanner.close();
  }

  /**
   * 创建表
   * @param tableName
   * @param family
   * @throws IOException
   */
  public static void createTable(Admin admin, String tableName, byte[] family) throws IOException {
    // 表的schema需要根据实际情况设置
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    htd.addFamily(new HColumnDescriptor(family));
    admin.createTable(htd);
  }

  /**
   * 停用表
   * @param tableName
   * @throws IOException
   */
  public static void disableTable(Admin admin, String tableName) throws IOException {
    admin.disableTable(TableName.valueOf(tableName));
  }

  /**
   * 启用表
   * @param tableName
   * @throws IOException
   */
  public static void enableTable(Admin admin, String tableName) throws IOException {
    admin.enableTable(TableName.valueOf(tableName));
  }

  /**
   * 删除表
   * @param tableName
   * @throws IOException
   */
  public static void deleteTable(Admin admin, String tableName) throws IOException {
    admin.deleteTable(TableName.valueOf(tableName));
  }



}
