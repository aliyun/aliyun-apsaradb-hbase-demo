"""
本实例为使用python 2.7 代码通过JDBC操作Spark, 需要用到python库jaydebeapi。
关于jaydebeapi的使用介绍请参考:https://pypi.org/project/JayDeBeApi/
运行此代码前请先设置环境变量export CLASSPATH=$CLASSPATH:/opt/jdbcjars/*
该目录中包含了JDBC依赖的所有jar包。内容如下请参考pom文件中依赖代码。
"""

import jaydebeapi
# ThriftServer访问地址，可以从Spark集群详情获取。使用时请把此路径替换为你自己的Spark集群的ThriftServer访问地址。
# 格式为：jdbc:hive2://xxx-001.spark.9b78df04-b.rds.aliyuncs.com:10000;
thriftServerAdress = 'jdbc:hive2://xxx:10000'
# HBase集群的ZK链接地址。使用时请把此路径替换为你自己的HBase集群的zk访问地址。
# 格式为：xxx-002.hbase.rds.aliyuncs.com,xxx-001.hbase.rds.aliyuncs.com,xxx-003.hbase.rds.aliyuncs.com:2181
zkAddress = 'xxx,xxx,xxx:2181'
conn=jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver",thriftServerAdress,["",""],"/opt/jdbcjars/hive-jdbc-1.2.1.spark2.jar")
curs = conn.cursor()
# Spark侧的表名。
sparkTableName = 'spark_phoenix'
# Phoenix侧的表名，需要在Phoenix侧提前创建。Phoenix表创建可以参考：https://help.aliyun.com/document_detail/53716.html?spm=a2c4g.11186623.4.2.4e961ff0lRqHUW
phoenixTableName = 'us_population';

create_cmd = ('CREATE TABLE ' + sparkTableName + 'USING org.apache.phoenix.spark\n'
         '	OPTIONS (\n'
         '	\'zkUrl\' = \'' + zkAddress + '\',\n'
         '	\'table\' = \'' + phoenixTableName + '\'\n'
         '	)')
print create_cmd
curs.execute(create_cmd)
curs.execute('select * from ' + sparkTableName + ' limit 1')
curs.fetchall()
curs.close()
conn.close()