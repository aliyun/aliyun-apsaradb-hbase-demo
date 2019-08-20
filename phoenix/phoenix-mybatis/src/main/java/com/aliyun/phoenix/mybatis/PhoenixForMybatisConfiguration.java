package com.aliyun.phoenix.mybatis;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;

import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.phoenix.queryserver.client.Driver;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

/**
 * @author 长轻
 * @date 2019/8/14 4:31 PM
 */
@Configuration
@MapperScan(
    basePackages = {PhoenixForMybatisConfiguration.PHOENIX_MYBATIS_MAPPER_PACKAGES},
    sqlSessionFactoryRef = PhoenixForMybatisConfiguration.PHOENIX_SQL_SESSION_FACTORY_BEAN_NAME)
public class PhoenixForMybatisConfiguration {

    static final String PHOENIX_MYBATIS_MAPPER_PACKAGES = "com.aliyun.phoenix.mybatis.mapper";

    //这里几个常量不用spring-mybatis自扫描的默认引用以及变量名字，是为了防止系统里有用mysql已经占用了默认的数据源，
    //从而导致引用无法正常注入或者选用正常的事务管理器
    static final String PHOENIX_SQL_SESSION_FACTORY_BEAN_NAME = "phoenixSqlSessionFactory";

    private final static String PHOENIX_TRANSACTION_MANAGER_NAME = "phoenix_transaction_manager";

    /**
     * mybatis mapper resource 路径
     */
    private static String MAPPER_PATH = "/sqlmapper/**.xml";

    @Value("${spring.datasource.phoenix.server.url}")
    private String phoenixServerUrl;

    @Bean
    public DataSource phoenixDataSource() {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setDriverClassName(Driver.class.getName());
        druidDataSource.setUrl("jdbc:phoenix:thin:url=" + phoenixServerUrl + ";serialization=PROTOBUF");
        return druidDataSource;
    }

    @Bean(name = PHOENIX_TRANSACTION_MANAGER_NAME)
    public DataSourceTransactionManager transactionManager() {
        return new DataSourceTransactionManager(phoenixDataSource());
    }

    @Bean(name = PHOENIX_SQL_SESSION_FACTORY_BEAN_NAME)
    public SqlSessionFactory sqlSessionFactoryBean()
        throws Exception {
        SqlSessionFactoryBean sqlSessionFactoryBean = new SqlSessionFactoryBean();
        //添加mapper 扫描路径
        PathMatchingResourcePatternResolver
            pathMatchingResourcePatternResolver = new PathMatchingResourcePatternResolver();
        sqlSessionFactoryBean.setMapperLocations(pathMatchingResourcePatternResolver.getResources(
            ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX + MAPPER_PATH));
        //设置datasource
        sqlSessionFactoryBean.setDataSource(phoenixDataSource());
        return sqlSessionFactoryBean.getObject();
    }

}
