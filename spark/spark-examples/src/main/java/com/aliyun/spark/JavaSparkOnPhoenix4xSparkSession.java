package com.aliyun.spark;

import org.apache.spark.sql.SparkSession;

/**
 * 本实例适用于Phoenix 4.x版本
 */
public class JavaSparkOnPhoenix4xSparkSession {
  public static void main(String[] args) {
    //HBase集群的ZK链接地址。
    //格式为：xxx-002.hbase.rds.aliyuncs.com,xxx-001.hbase.rds.aliyuncs.com,xxx-003.hbase.rds.aliyuncs.com:2181
    String zkAddress = args[0];
    //Phoenix侧的表名，需要在Phoenix侧提前创建。Phoenix表创建可以参考：https://help.aliyun.com/document_detail/53716.html?spm=a2c4g.11186623.4.2.4e961ff0lRqHUW
    String phoenixTableName = args[1];
    //Spark侧的表名。
    String sparkTableName = args[2];

    SparkSession sparkSession = SparkSession
            .builder()
            .enableHiveSupport() //使用hive-metastore后通过beenline可以查看到代码中创建的表
            .appName("java spark on Phoenix test")
            .getOrCreate();

    String createCmd = "CREATE TABLE " + sparkTableName + " USING org.apache.phoenix.spark\n" +
            "OPTIONS (\n" +
            "  'zkUrl' '" + zkAddress + "',\n" +
            "  'table' '" + phoenixTableName + "'\n" +
            ")";
    System.out.println(" createCmd: \n" + createCmd);
    sparkSession.sql(createCmd);
    String querySql = "select * from " + sparkTableName + " limit 1";
    sparkSession.sql(querySql).show();
    sparkSession.stop();
  }
}
