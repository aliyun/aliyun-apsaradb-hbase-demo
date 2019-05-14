# -*- coding: UTF-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# spark 作业控制台内容如下：
# /spark_on_maxcompute/SparkOnMaxcomputeSparkSession.py
# --py-files /spark_on_maxcompute/odps.py
# --jars /spark_on_maxcompute/spark-examples-0.0.1-SNAPSHOT-shaded.jar
# --driver-memory 1G
# --driver-cores 1
# --executor-cores 1
# --executor-memory 2G
# --num-executors 1
# --name spark_on_maxcompute
# sparktest2(maxcompute 表名) accessid accesskey
# http://service.cn.maxcompute.aliyun-inc.com/api http://dt.cn-shenzhen.maxcompute.aliyun-inc.com spark_on_maxcompute(maxcompute 工作空间名称)
# pt='2018-04-01'

# 本实例使用的表sparktest2由下面代码创建
# https://github.com/aliyun/aliyun-apsaradb-hbase-demo/blob/master/spark/spark-examples/src/main/scala/com/aliyun/spark/maxcompute/MaxComputeDataSourcePartitionSample.scala
# 依赖的spark-examples-0.0.1-SNAPSHOT-shaded.jar由spark-examples工程编译得到

import sys

from odps import OdpsOps
from pyspark.sql import SparkSession

if __name__ == "__main__":

    table = sys.argv[1]
    accessKeyId = sys.argv[2]
    accessKeySecret = sys.argv[3]
    odpsUrl = sys.argv[4]
    tunnelUrl = sys.argv[5]
    project = sys.argv[6]
    partition = sys.argv[7]
    numPartitions = 2

    print(partition)

    spark = SparkSession \
        .builder \
        .appName("SparkOnMaxcomputeDemo") \
        .getOrCreate()

    sc = spark.sparkContext

    odpsOps = OdpsOps(sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl)

    nump = int(numPartitions)
    rdd = odpsOps.readPartitionTable(project, table, partition, nump, batchSize=1)
    rows = rdd.collect()
    for row in rows:
        print "row: ",
        for col in row:
            print col, type(col),
        print ""