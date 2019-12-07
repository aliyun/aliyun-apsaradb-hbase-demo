# -*- coding: UTF-8 -*-
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import *

if __name__ == "__main__":
    # 读取kafka的配置
    fromBroker = sys.argv[1]
    fromTopic = sys.argv[2]
    # 写入kakfa的配置
    toBroker = sys.argv[3]
    toTopic = sys.argv[4]

    #初始化sparkSession
    spark = SparkSession.builder.appName("fromKakfaToKafka").getOrCreate()

    fields = [StructField("station", StringType(), True), \
              StructField("city", StringType(), True), \
              StructField("population", IntegerType(), True)]
    schema = StructType(fields)

    #读取structuredStreming kafka 并处理逻辑
    data = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", fromBroker) \
        .option("subscribe", fromTopic) \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .createOrReplaceTempView("inputTable")

    #value 的值：HI,city_000000000000002,9845430
    #写入到kafka的DataFrame需要有key和value两个字段。
    outputData = spark.sql("select concat('key:', rand()) as key, value as value from inputTable")

    #打印到控制台
    # outputData.writeStream \
    #     .outputMode('append') \
    #     .format('console') \
    #     .start() \
    #     .awaitTermination()

    #写入structuredStreming kafka
    outputData.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", toBroker) \
        .option("topic", toTopic) \
        .option("checkpointLocation", "/tmp/kafka-checkpoint/test002/") \
        .trigger(continuous="10 second") \
        .start() \
        .awaitTermination()



