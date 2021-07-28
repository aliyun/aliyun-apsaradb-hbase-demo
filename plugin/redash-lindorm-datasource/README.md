通过Redash访问Lindorm
======================================

Redash是一款开源的BI工具，提供了基于web的数据库查询和数据可视化功能，本文介绍如何使用Redash连接Lindorm。

前提条件
-------------------------

已安装Redash，具体信息，请参见[Redash官网](https://redash.io/)。

安装插件
-------------------------

* 如果用源码的方式部署Redash，则需要将代码的根目录作为`$REDASH_SETUP_FOLDER`。
* 如果用拉取Docker镜像方式部署Redash，则需要将redash_server，redash_scheduler，redash_adhoc_worker，redash_scheduled_worker四个容器中的 */app* 目录作为`$REDASH_SETUP_FOLDER`，具体操作如下：
    1. 下载 *Lindorm.png* 和 *lindorm.py* 文件，下载链接，请参见[redash](https://github.com/aliyun/aliyun-apsaradb-hbase-demo/tree/master/plugin/redash-lindorm-datasource)。

    2. 将 *Lindorm.png* 文件复制到 *REDASH_SETUP_FOLDER/client/app/assets/images/db-logos/* 和 *REDASH_SETUP_FOLDER/client/dist/images/db-logos/* 目录下。

    3. 将 *lindorm.py* 文件复制到 *$REDASH_SETUP_FOLDER/redash/query_runner/* 目录下。

    4. 在文件 *$REDASH_SETUP_FOLDER/redash/settings/__init__.py* 中按如下方式增加一行。

           default_query_runners = [
             'redash.query_runner.athena',
             'redash.query_runner.big_query',
             ........
             'redash.query_runner.uptycs',
             'redash.query_runner.lindorm',      # Add this line
           ]
    5. 在运行Redash的Python环境中安装phoenixdb==1.0.1（Docker镜像方式部署同样需要对redash_server，redash_scheduler，redash_adhoc_worker，redash_scheduled_worker四个容器执行此操作）。以下是Ubuntu中的安装代码示例：

            apt update && apt install libkrb5-dev && pip install phoenixdb==1.0.1




5. 重启Redash。









配置数据源
--------------------------

1. 登录Redash。



2. 单击页面右上角的![Redash设置图标](https://static-aliyun-doc.oss-accelerate.aliyuncs.com/assets/img/zh-CN/0463223261/p282408.png) 图标。



3. 在 **Data Sources** 页签，单击 **New data source** 。



4. 在 **New data source** 页面，单击Lindorm对应的图标。



5. 配置Lindorm的连接地址、端口。

   **说明** 请使用宽表SQL访问地址和端口。


6. 单击 **Create** 。

   您可以通过 **Test Connection** 检测连接状态。




操作示例
-------------------------

如果您想绘制一张包含各地气温随时间变化的曲线图表，数据库按照如下方式设计，包含气温（temp）、测试时间（local_time）、城市（city）、索引（id）这几个字段：

    CREATE TABLE log.temp
    (
        local_time timestamp,
        temp int,
        id int,
        city text,
        primary key ((id))
    )



1. 在查询编辑器（Query Editor）页面，按如下方式编写CQL语句。

       SELECT city, temp, local_time FROM temp



2. 查询的数据如下。

   ![对接Redash-1](https://github.com/aliyun/aliyun-apsaradb-hbase-demo/raw/master/plugin/redash-lindorm-datasource/img/usage_hint_1.PNG)

3. 单击 **New Visualization** 。

   **X Column** 选择`local_time`， **Y Columns** 选择`temp`， **Group by** 选择`City`，绘制的图标如下。![对接Redash-2](https://github.com/aliyun/aliyun-apsaradb-hbase-demo/raw/master/plugin/redash-lindorm-datasource/img/usage_hint_2.PNG)
