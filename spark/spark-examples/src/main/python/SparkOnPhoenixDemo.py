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

from __future__ import print_function

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("SparkOnPhoenixDemo")\
        .getOrCreate()
    #spark创建临时表去分析phoenix的数据,下面的zkUrl填写对应hbase集群的zk连接地址
    zkURL = ""
    df = spark.read \
        .format("org.apache.phoenix.spark") \
        .option("table", "US_POPULATION") \
        .option("zkUrl", zkURL) \
        .load()
    df.registerTempTable("pyspark_us_population")
    spark.sql("select * from pyspark_us_population").show()


    #spark创建外表关联phoenix表，然后分析phoenix数据
    tableName  = "US_POPULATION"
    createTableString = "CREATE TABLE pyspark_us_population " \
                        "USING org.apache.phoenix.spark OPTIONS(\'zkUrl\' \'%s\', \'table\' \'%s\')" % (zkURL, tableName)
    spark.sql(createTableString)
    spark.sql("select * from pyspark_us_population").show()

    spark.stop()

