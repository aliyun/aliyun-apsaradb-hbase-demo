
//准备工作，在phoenix集群创建测试表us_population
//参考文档：https://help.aliyun.com/document_detail/53716.html?spm=a2c4g.11186623.6.592.2d9d554fxfNzFC


//1、spark创建临时表去分析phoenix的数据,下面的zkUrl填写对应hbase集群的zk连接地址


%livy.spark
val cat= s"""{
  "table":{"namespace":"default", "name":"test"},"rowkey":"key",
  "columns":{
  "key":{"cf":"rowkey", "col":"key", "type":"string"},
  "value":{"cf":"cf", "col":"a", "type":"string"}}}
  """

#下面的zkURL填写对应hbase集群的zk地址
val zkUrl = ""

val df = spark.sqlContext.load(
  "org.apache.hadoop.hbase.spark",
  Map("catalog" -> cat, "hbase.zookeeper.quorum" -> zkUrl
  )
)
df.registerTempTable("spark_test")
spark.sql("select * from spark_test").show()



//2. spark创建外表关联phoenix表，然后分析phoenix数据
%livy.spark
val sparkTableName = "spark_hbase_test"
val cat= s"""{
  "table":{"namespace":"default", "name":"test"},"rowkey":"key",
  "columns":{
  "key":{"cf":"rowkey", "col":"key", "type":"string"},
  "value":{"cf":"cf", "col":"a", "type":"string"}}}
  """

#下面的zkURL填写对应hbase集群的zk地址
val zkUrl = ""
val createTable:String = s"""CREATE TABLE %s USING org.apache.hadoop.hbase.spark
                              OPTIONS ('catalog'= '%s',
                              'hbase.zookeeper.quorum' = '%s'
                              )""".format(sparkTableName,cat,zkUrl)
spark.sql(createTable)
spark.sql("select * from spark_hbase_test").show()


