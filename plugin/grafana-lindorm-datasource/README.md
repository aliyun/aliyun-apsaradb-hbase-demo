通过Grafana访问Lindorm
=======================================

Grafana是指标数据图形化展示工具，可以从多个数据源采集数据并以图表展示。本文介绍如何通过Grafana访问Lindorm中的数据，可以利用Grafana丰富易用的可视化工具更好地监控和分析来自Lindorm的数据。

前提条件
-------------------------

* 已安装7.x-8.x版本的Grafana，具体操作，请参见[下载Grafana](Grafana.com/grafana/download?platform=windows)。

* 该数据源只适用于TableService结构的时序数据（time-series data）。




安装插件
-------------------------

1. 进入Grafana插件安装目录。

       cd *


**说明** 如果在Ubuntu系统中，需要在 */var/lib/grafana/plugins/* 中安装插件。


2. 执行安装插件命令。

       wget https://github.com/aliyun/aliyun-apsaradb-hbase-demo/raw/master/plugin/grafana-lindorm-datasource/release/grafana-lindorm-datasource.zip
       unzip grafana-lindorm-datasource.zip



3. 在已部署Grafana的机器中，打开 *grafana.ini* 配置文件。
   * MacOS系统的文件路径： */usr/local/etc/grafana/grafana.ini* 。

   * Linux系统的文件路径： */etc/grafana/grafana.ini* 。




4. 在 *grafana.ini* 配置文件中的plugins项设置allow_loading_unsigned_plugins参数。

       allow_loading_unsigned_plugins = lindorm_lql_datasource



5. 重启Grafana服务。

       service grafana-server restart






配置数据源
--------------------------

1. 登录Grafana服务。

2. 在左侧菜单栏，选择**Settings**\> **Data Sources** 。

3. 在 **Data Sources** 页签，单击 **Add data source** 。

4. 在 **Add data source** 页面，单击 **Lindorm Datasource** 对应的 **Select** 按钮。

5. 配置数据源参数，Path项填入通讯地址和端口（使用宽表SQL访问地址及端口，如"http://xxx.aliyuncs.com:30060"）。




操作示例
--------------------------

Data source选择在上一步添加的数据源，左侧插件返回的数据有Table(表格)和Time Series(时序)可选，可根据图表类型进行选择。此处我们选择Time Series，进行时序图表的添加。

假如想绘制一张包含各地气温随时间变化曲线的图表。Lindorm中的数据如下，包括气温temp，测量时间local_time，城市city和索引id。

    create table temp
    (
        local_time timestamp,
        temp int, 
        id int, 
        city text, 
        primary key ((id))
    )

文本框内填写查询语句，兼容Lindorm SQL语法。更多信息，请参见[Lindorm SQL介绍](https://help.aliyun.com/document_detail/264918.html?spm=a2c4g.11186623.6.569.7aac35c4huBQJf)。
查询语句如下：

    SELECT city,temp,local_time from temp

Time formatted columns中填写时序字段对应的列，此处为local_time。

查询到的数据如下图所示。![shujuxianshi](https://github.com/aliyun/aliyun-apsaradb-hbase-demo/raw/master/plugin/grafana-lindorm-datasource/img/temp_table.PNG)

在 **Visualization** 中选择 **Time Series** ，并调整时间段，得到的图表如下所示。![tubiaoxianshi](https://github.com/aliyun/aliyun-apsaradb-hbase-demo/raw/master/plugin/grafana-lindorm-datasource/img/temp_query.PNG)
