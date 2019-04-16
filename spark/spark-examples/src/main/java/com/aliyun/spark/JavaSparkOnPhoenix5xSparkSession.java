package com.aliyun.spark;

import org.apache.spark.sql.SparkSession;

/**
 * 运行命令
 * spark-submit --master yarn --class com.aliyun.spark.JavaSparkOnPhoenix5xSparkSession /opt/spark-examples-0.0.1-SNAPSHOT.jar zkAddress
 */
public class JavaSparkOnPhoenix5xSparkSession {
  public static void main(String[] args) {
    SparkSession sparkSession = SparkSession
            .builder()
            .enableHiveSupport() //使用hive-metastore后通过beenline可以查看到代码中创建的表
            .appName("java spark on Phoenix5.x test")
            .getOrCreate();

    //Spark侧的表名。
    String sparkTableName = "spark_phoenix";
    //Phoenix侧的表名，需要在Phoenix侧提前创建。Phoenix表创建可以参考：https://help.aliyun.com/document_detail/53716.html?spm=a2c4g.11186623.4.2.4e961ff0lRqHUW
    String phoenixTableName = "us_population";

    //queryServerAddress为HBase集群SQL服务访问地址，格式：http://xxx：8765
    String queryServerAddress = args[0];
    String url = "jdbc:phoenix:thin:url=" + queryServerAddress + ";serialization=PROTOBUF";
    String driver = "org.apache.phoenix.queryserver.client.Driver";
    String createCmd = "CREATE TABLE " +
            sparkTableName +
            " USING org.apache.spark.sql.jdbc\n" +
            " OPTIONS (\n" +
            "  'driver' '" + driver + "',\n" +
            "  'url' '" + url + "',\n" +
            "  'dbtable' '" + phoenixTableName + "',\n" +
            "  'fetchsize' '" + 100 + "'\n" +
            ")";
    System.out.println(" createCmd: \n" + createCmd);
    sparkSession.sql(createCmd);
    String querySql = "select * from " + sparkTableName + " limit 1";
    sparkSession.sql(querySql).show();
    sparkSession.stop();
  }
}
