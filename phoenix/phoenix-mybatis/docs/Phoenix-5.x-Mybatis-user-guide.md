# Mybatis 适配Phoenix-5.x使用说明
##本demo工程测试用例使用说明

一.将application.properties里面的键spring.datasource.phoenix.server.url所对应的值给改成自己的HBase SQL服务的客户端访问地址。
 
二.测试用例类为 com.aliyun.phoenix.mybatis.UsPopulationMapperTest



##Mybatis接入注意事项

一.如果接入项目中使用了除了phoenix数据源还有其他数据源，要注意相关spring bean的冲突，例如两个数据源事务管理器要指定某一个去使用，数据源对象要指定某一个去使用。在本demo工程中是将phoenix相关的bean
对象重命名以防止冲突，具体可以看类com.aliyun.phoenix.mybatis.PhoenixForMybatisConfiguration中的配置
