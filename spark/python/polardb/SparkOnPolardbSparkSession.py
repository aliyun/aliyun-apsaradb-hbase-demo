#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
X-Pack Spark 控制台提交命令示例

--driver-memory 1G
--driver-cores 1
--executor-cores 1
--executor-memory 1G
--num-executors 1
--jars /mysql-connector-java-5.1.34.jar
--name on_polardb
/SparkOnPolardbSparkSession.py
${polardb的访问地址} ${polardb的数据库名} ${polardb的表名} ${登录polardb数据库的用户名} ${登录polardb数据库的密码} ${Spark侧的表名}
"""
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":

    #  获取POLARDB的 url、database、tableName、登录POLARDB数据库的user和password
    url = sys.argv[1]
    jdbcConnURL = "jdbc:mysql://" + url
    database = sys.argv[2]
    tableName = sys.argv[3]
    user = sys.argv[4]
    password = sys.argv[5]
    driver = "com.mysql.jdbc.Driver"
    # Spark侧的表名。
    sparkTableName = sys.argv[6]

    spark = SparkSession\
        .builder\
        .appName("spark on hbase")\
        .getOrCreate()

    create_cmd = ('CREATE TABLE ' + sparkTableName + ' USING org.apache.spark.sql.jdbc\n'
             '	OPTIONS (\n'
             '      driver \'' + driver + '\',\n'
             '      url \'' + jdbcConnURL + '\',\n'
             '      dbtable \'' + database + '.' + tableName + '\',\n'
             '      user \'' + user + '\',\n'
             '      password \'' + password + '\'\n'
             '	)')
    print(create_cmd)
    spark.sql(create_cmd)
    query = "select * from " + sparkTableName + " limit 10"
    print(query)
    spark.sql(query).show()
    spark.stop()