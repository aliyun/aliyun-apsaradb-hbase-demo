# Phoenix-5.x使用说明

Phoenix-5.x对应HBase-2.x

### 使用步骤

1. 开通Phoenix-5.x

  blabla 操作步骤

2. 获取连接串

  截图
  配置白名单

3. 使用客户端访问

  下载
  url

  启动sqlline

  验证

  退出

4. 使用SDK访问

  maven依赖

  demo url

### 同Phoenix-4.x的异同

####相同点

SQL语法和API相同，用户无需修改原有代码。

####不同点

1. Phoenix-5.x对于时区的处理逻辑做了统一优化，参考云栖文章：[Phoenix关于时区的处理方式说明](https://yq.aliyun.com/articles/684390)。
2. Phoenix-5.x默认提供轻客户端模式，而Phonix-4.x默认提供重客户端模式。url