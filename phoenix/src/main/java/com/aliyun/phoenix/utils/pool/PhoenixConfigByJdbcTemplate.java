package com.aliyun.phoenix.utils.pool;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * phoenix客户端连接
 *
 * @author mengqingyi 2018年8月27日10:13:22
 */
@Configuration
public class PhoenixConfigByJdbcTemplate {

    //读取配置文件
    //@Value("${phoenix_url}")
    //置于工具类中
    @Value("XXX")
    private String url;

    /**
     * 连接池基本配置
     * @return phoenixDataSource
     */
    public BasicDataSource phoenixDataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver");
        dataSource.setInitialSize(0);
        dataSource.setMinIdle(0);
        dataSource.setMaxActive(50);
        dataSource.setMaxWait(5000);
        dataSource.setTimeBetweenEvictionRunsMillis(1000);
        dataSource.setMinEvictableIdleTimeMillis(5000);
        dataSource.setUrl(url);
        return dataSource;
    }

    @Bean
    public JdbcTemplate phoenixJdbcTemplate() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(phoenixDataSource());
        return jdbcTemplate;
    }

}
