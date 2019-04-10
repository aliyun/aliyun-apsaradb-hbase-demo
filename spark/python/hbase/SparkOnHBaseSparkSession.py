#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # HBase集群的ZK链接地址。//HBase集群的ZK链接地址。使用时请把此路径替换为你自己的HBase集群的zk访问地址。
    # 格式为：xxx-002.hbase.rds.aliyuncs.com:2181,xxx-001.hbase.rds.aliyuncs.com:2181,xxx-003.hbase.rds.aliyuncs.com:2181
    zkAddress = sys.argv[1]
    # hbase侧的表名，需要在hbase侧提前创建。hbase表创建可以参考：https://help.aliyun.com/document_detail/52051.html?spm=a2c4g.11174283.6.577.7e943c2eiYCq4k
    hbaseTableName = sys.argv[2]
    # Spark侧的表名。
    sparkTableName = sys.argv[3]

    spark = SparkSession\
        .builder\
        .appName("spark on hbase")\
        .getOrCreate()

    create_cmd = ('CREATE TABLE ' + sparkTableName + ' USING org.apache.hadoop.hbase.spark\n'
             '	OPTIONS (\'catalog\'= \n'
             '	\'{\"table\":{\"namespace\":\"default\", \"name\":\"' + hbaseTableName + '\"},\"rowkey\":\"rowkey\", \n'
             '	\"columns\":{ \n'
             '	\"col0\":{\"cf\":\"rowkey\", \"col\":\"rowkey\", \"type\":\"string\"},\n'
             '	\"col1\":{\"cf\":\"cf\", \"col\":\"col1\", \"type\":\"String\"}}}\',\n'
             '	\'hbase.zookeeper.quorum\' = \'' + zkAddress + '\'\n'
             '	)')
    print(create_cmd)
    spark.sql(create_cmd)
    query = "select * from " + sparkTableName + " limit 10"
    print(query)
    spark.sql(query).show()
    spark.stop()