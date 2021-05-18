# Lindorm对接Grafana

Grafana接入Lindorm数据源的插件，该数据源只适用于使用TableService结构的时序数据(time-series data)。

## 前提条件

* 已安装Grafana


## 安装插件

1. 执行如下命令进入Grafana插件安装目录。

   例如在Ubuntu系统，您需要在/var/lib/grafana/plugins/中安装插件。

    ```
    cd /var/lib/grafana/plugins/
    ```

2. 执行如下命令安装插件。

    ```
    wget https://github.com/aliyun/aliyun-apsaradb-hbase-demo/raw/master/plugin/grafana-lindorm-datasource/release/grafana-lindorm-datasource.zip
    unzip grafana-lindorm-datasource.zip
    ```

3. 在已部署 Grafana 的机器中，打开 grafana.ini 配置文件。
   
   MacOS系统的文件路径：/usr/local/etc/grafana/grafana.ini
   
   Linux系统的文件路径：/etc/grafana/grafana.ini

   在 plugins中设置 allow_loading_unsigned_plugins 参数。
   
    ```
    allow_loading_unsigned_plugins = aliyun-lindorm-datasource
    ```

4. 重启 grafana 服务。

    ```
    service grafana-server restart
    ```
   
## 配置数据源

1. 登录Grafana。
2. 在左侧菜单栏，选择<img src="https://static-aliyun-doc.oss-accelerate.aliyuncs.com/assets/img/zh-CN/7664559951/p112522.png"> > Data Sources。
3. 在Data Sources页签，单击Add data source。
4. 在Add data source页面，单击Alibaba Lindorm对应的Select。
5. 配置数据源的通讯地址和端口，（如"10.11.12.13:9042"，使用CQL访问地址及端口）以及用户名和密码。keyspace字段可为空。

## 添加仪表盘

可以使用Query Configurator和Query Editor两种方式查询数据。Configurator更易于使用但存在一定的限制，Editor需要熟悉CQL语言。

### Query Configurator

Query Configurator是查询数据的最简单方法。首先，输入keyspace和table name，然后选择适当的columns。如果keyspace和table name被正确给出，数据源将自动建议columns名称。

* Time Column - 存储timestamp value的Column，用于回答 "何时 "问题。
* Value Column -存储你想显示的值的Column。它可以是数值、温度或任何你需要的属性。
* ID Column -唯一标识数据来源的Column，例如，sensor_id、shop_id或任何允许你识别数据来源的东西。

之后，你必须指定想显示的数据来源的特定ID值。你可能需要启用 "ALLOW FILTERING"，尽管我们建议避免它。

### Query Editor

Query Editor是查询数据的更强大的方式。要启用Query Editor，请点击"切换文本编辑模式"按钮。

<img src="https://user-images.githubusercontent.com/1742301/102781863-a8bd4b80-4398-11eb-8c28-4d06a1f29279.png" height="150">

Query Editor兼容CQL的语法，详见Lindorm官方文档。

例如：
```
SELECT id, CAST(value as double), created_at FROM test.test WHERE id IN (99051fe9-6a9c-46c2-b949-38ef78858dd1, 99051fe9-6a9c-46c2-b949-38ef78858dd0) AND created_at > $__timeFrom and created_at < $__timeTo
```

遵循SELECT表达式的顺序:
1. ID--SELECT表达式中的第一个属性必须是ID，是唯一标识数据的东西（例如：sensor_id）。
2. Value - 第二个属性必须是你要显示的值
3. Timestamp - 第三个值必须是值的timestamp。所有其他属性将被忽略。

要按时间过滤数据，使用$__timeFrom和$__timeTo占位符，就像例子中一样。数据源将用panel上的时间值替换它们。注意 添加占位符很重要，否则查询将试图获取整个时间段的数据。不要试图自己指定时间范围，只需添加占位符，指定时间限制是grafana的工作。

### 操作示例

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
SELECT city, CAST(temp as double), local_time FROM log.temp WHERE local_time > $__timeFrom and local_time < $__timeTo  ALLOW FILTERING
```
查询到的数据如下：

<img src="https://github.com/aliyun/aliyun-apsaradb-hbase-demo/raw/master/img/temp_table.PNG">

在Visualization中选择Graph，并调整时间段，得到的图表如下所示：

<img src="https://github.com/aliyun/aliyun-apsaradb-hbase-demo/raw/master/img/temp_query.PNG">

