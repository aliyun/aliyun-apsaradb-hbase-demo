//准备工作，在phoenix集群创建测试表us_population
//参考文档：https://help.aliyun.com/document_detail/53716.html?spm=a2c4g.11186623.6.592.2d9d554fxfNzFC


//1、spark创建临时表去分析phoenix的数据,下面的zkUrl填写对应hbase集群的zk连接地址

%livy.spark
#下面的zkURL填写对应hbase集群的zk地址
val zkUrl = ""
val df = spark.sqlContext.load(
  "org.apache.phoenix.spark",
  Map("table" -> "US_POPULATION", "zkUrl" ->
    zkUrl
  )
)
df.registerTempTable("spark_us_population")
spark.sql("select * from spark_us_population").show()




//2. spark创建外表关联phoenix表，然后分析phoenix数据
%livy.spark
#下面的zkURL填写对应hbase集群的zk地址
val zkURL:String =  ""
val tableName:String = "US_POPULATION"
val createSparkTable:String = ("CREATE TABLE spark_us_population USING org.apache.phoenix.spark " +
  "OPTIONS('zkUrl' '%s', 'table' '%s') ").format(zkURL,tableName)
spark.sql(createSparkTable)
spark.sql("select * from spark_us_population").show()


