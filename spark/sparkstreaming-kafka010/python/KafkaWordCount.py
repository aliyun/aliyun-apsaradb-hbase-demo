# -*- coding: UTF-8 -*-
import sys
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

'''
此实例适用于阿里云互联网中间件->消息队列kafka。
提交命令如下，这里需要用到spark-streaming-kafka-0-8-assembly_2.11-2.3.2.jar，
curl -X POST --data '{"file": "/resourcesdir/SparkStreaming.py", "args":["ip1:9092,ip2:ip3:9092", "topic", "groupid"], "jars": ["/resourcesdir/spark-streaming-kafka-0-8-assembly_2.11-2.3.2.jar"]}' -H "Content-Type: application/json" http://xxx:8998/batches
'''

def updateFunc(values, state):
    if state is None:
        state = 0
    return sum(values, state)

conf = SparkConf()
sc = SparkContext(conf = conf)
ssc = StreamingContext(sparkContext = sc, batchDuration=10)
#ssc.checkpoint('/tmp/testpython')

# kafka的broker，在阿里云互联网中间件->消息队列kafka中，topic管理或者 Consumer Group管理中的"获取接入点"中获取。
# 格式为：ip:port,ip:port,ip:port
brokers = sys.argv[1]
print 'brokers=' + brokers
# kafka的 topic，在阿里云互联网中间件->消息队列kafka中，topic管理中创建。
topics = sys.argv[2]
# kafka GroupID，可从kafka服务的Consumer Group 管理创建获取。
groupId = sys.argv[3]
# 设置kafka参数，并创建Dstream
kvs = KafkaUtils.createDirectStream(
    ssc = ssc,
    topics = topics.split(","),
    kafkaParams = {"metadata.broker.list": brokers,
                   "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
                   "key.deserializer":"org.apache.kafka.common.serialization.StringDeserializer",
                   "group.id": groupId}
)

# 计算单词个数。
lines = kvs.map(lambda x: x[1])
wordCounts = lines.flatMap(lambda  line: line.split(' '))\
    .map(lambda  word: (word,1))\
    .updateStateByKey(updateFunc)

wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
