
#准备工作，在phoenix集群创建测试表us_population
#参考文档：https://help.aliyun.com/document_detail/53716.html?spm=a2c4g.11186623.6.592.2d9d554fxfNzFC


#1、spark创建临时表去分析phoenix的数据,下面的zkUrl填写对应hbase集群的zk连接地址
%livy.pyspark
#下面的zkURL填写对应hbase集群的zk地址
zkURL = ""

df = spark.read \
    .format("org.apache.phoenix.spark") \
    .option("table", "US_POPULATION") \
    .option("zkUrl", zkURL) \
    .load()
df.registerTempTable("pyspark_us_population")
spark.sql("select * from pyspark_us_population").show()


#3、spark创建外表关联phoenix表，然后分析phoenix数据
%livy.pyspark
zkURL = ""
tableName  = "US_POPULATION"
createTableString = "CREATE TABLE pyspark_us_population " \
                    "USING org.apache.phoenix.spark OPTIONS(\'zkUrl\' \'%s\', \'table\' \'%s\')" % (zkURL, tableName)
spark.sql(createTableString)
spark.sql("select * from pyspark_us_population").show()
