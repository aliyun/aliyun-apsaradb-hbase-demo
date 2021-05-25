# Lindorm对接Redash

## 安装插件

* 若为拉取Docker镜像方式部署Redash需要将redash_server，redash_scheduler，redash_adhoc_worker，redash_scheduled_worker四个容器中的/app目录都作为$REDASH_SETUP_FOLDER执行如下操作

* 若为源代码部署方式$REDASH_SETUP_FOLDER为代码的根目录

1. 将文件 Lindorm.png 复制到 $REDASH_SETUP_FOLDER/client/app/assets/images/db-logos/和$REDASH_SETUP_FOLDER/client/dist/images/db-logos/目录下

2. 将文件 lindorm.py 复制到 $REDASH_SETUP_FOLDER/redash/query_runner/

3. 在文件 $REDASH_SETUP_FOLDER/redash/settings/\_\_init\_\_.py 中按如下方式增加一行:

```py
default_query_runners = [
  'redash.query_runner.athena',
  'redash.query_runner.big_query',
  ........
  'redash.query_runner.uptycs',
  'redash.query_runner.lindorm',      # Add this line
]
```

4. 重启Redash

## 配置数据源

1. 登录Redash。
2. 点击设置，在Data Sources页签，单击New data source。
3. 在New data source页面，单击Lindorm对应的Select。
4. 配置数据源的通讯地址和端口，（如Host"10.11.12.13"，Port填写9042，使用CQL访问地址及端口）以及用户名和密码。
5. 点击Create，可以Test Connection检测连接情况。

## 操作示例

假如想绘制一张包含各地气温随时间变化曲线的图标，数据库按照如下方式设计，包含temp气温，local_time测量时间，city城市,id索引这几个字段：
```
create table log.temp
(
	local_time timestamp,
	temp int, 
	id int, 
	city text, 
	primary key ((id))
)
```
在Query Editor中，我们按如下方式编写CQL语句:
```
SELECT city, temp, local_time FROM temp
```
查询到的数据如下：

<img src="https://github.com/aliyun/aliyun-apsaradb-hbase-demo/raw/master/plugin/redash-lindorm-datasource/img/usage_hint_1.PNG">

点击New Visualization，X Column选择local_time，Y Columns选择temp，Group by选择City，绘制的图表如下：

<img src="https://github.com/aliyun/aliyun-apsaradb-hbase-demo/raw/master/plugin/redash-lindorm-datasource/img/usage_hint_2.PNG">





