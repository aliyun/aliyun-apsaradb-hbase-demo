# -*- coding: UTF-8 -*-
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *

'''
此实例适用于阿里云互联网中间件->消息队列kafka。
提交命令如下，这里需要用到spark-streaming-kafka-0-8-assembly_2.11-2.3.2.jar，
curl -X POST --data '{"file": "/resourcesdir/SparkstreamingOnKafkaWriteToParquet.py", "args":["ip1:9092,ip2:9092,ip3:9092", "topicName", "groupName","parquetFilePath"], "jars": ["/resourcesdir/spark-streaming-kafka-0-8-assembly_2.11-2.3.2.jar"]}' -H "Content-Type: application/json" http://xxx:8998/batches
'''
def process_data(rdd):
    try:
        # 创建spark session
        if "sparkSession" not in globals():
            globals()["sparkSession"] = SparkSession \
                .builder \
                .appName("write to parquet test") \
                .getOrCreate()
        # 定义schema，最后一列示例数组的定义。
        fields = [StructField("name", StringType(), True),StructField("counts", StringType(), True),StructField("intarray", ArrayType(IntegerType()), True)]
        schema = StructType(fields)
        # 创建dataframe
        df = sparkSession.createDataFrame(rdd,schema)
        df.show()
        df.write.mode("append").parquet(parquetFilePath)

        # read the parquet data
        print '========start to read the parquet file===== '
        parquetDf = sparkSession.read.parquet(parquetFilePath)
        # 注册成spark表
        parquetDf.registerTempTable("test_table")
        sparkSession.sql("select * from test_table limit 10").show
        #parquetDf.show()
        parquetDf.printSchema
    except:
        print 'some error'

conf = SparkConf()
sc = SparkContext(conf = conf)
ssc = StreamingContext(sparkContext = sc, batchDuration=20)
#ssc.checkpoint('/tmp/testpython')

# kafka的broker，在阿里云互联网中间件->消息队列kafka中，topic管理或者 Consumer Group管理中的"获取接入点"中获取。
# 格式为：ip:port,ip:port,ip:port
brokers = sys.argv[1]
print 'brokers=' + brokers
# kafka的 topic，在阿里云互联网中间件->消息队列kafka中，topic管理中创建。
topics = sys.argv[2]
# kafka GroupID，可从kafka服务的Consumer Group 管理创建获取。
groupId = sys.argv[3]
# parquet 文件存储地址，例如：hdfs://hb-xxx/test/tablea/
parquetFilePath = sys.argv[4]
# 设置kafka参数，并创建Dstream
kvs = KafkaUtils.createDirectStream(
    ssc = ssc,
    topics = topics.split(","),
    kafkaParams = {"metadata.broker.list": brokers,
                   "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
                   "key.deserializer":"org.apache.kafka.common.serialization.StringDeserializer",
                   "group.id": groupId}
)

lines = kvs.map(lambda x: x[1])
wordCounts = lines.flatMap(lambda  line: line.split(' '))\
    .map(lambda  word: Row(word,1,[1,2,3]))
wordCounts.foreachRDD(lambda rdd: process_data(rdd))

ssc.start()
ssc.awaitTermination()