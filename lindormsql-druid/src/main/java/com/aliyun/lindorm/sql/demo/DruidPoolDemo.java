package com.aliyun.lindorm.sql.demo;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

/**
 * @author jianhong.hjh
 * @date 2022/7/27 2:48 PM
 */
public class DruidPoolDemo {
  private static final Logger LOG = LoggerFactory.getLogger(DruidPoolDemo.class);

  public static void main(String[] args) throws Exception {
    // 加载参数
    Properties properties = new Properties();
    InputStream inputStream = DruidPoolDemo.class.getClassLoader().getResourceAsStream("druid.properties");
    properties.load(inputStream);
    // 初始化连接池
    DataSource dataSource = DruidDataSourceFactory.createDataSource(properties);

    /* -------------- 基于JDBC的CRUD示例 ----------------- */

    String tableName = "sql_table_" + new Random().nextInt(1000);
    // 创建表
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        String sql = "create table if not exists " + tableName + "(id VARCHAR, name VARCHAR, primary key(id))";
        int ret = statement.executeUpdate(sql);
        System.out.println(ret);
      }
    }

    // 插入数据
    try (Connection connection = dataSource.getConnection()) {
      String sql = "upsert into " + tableName + "(id,name) values(?,?)";
      try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.setString(1, "aa");
        ps.setString(2, "bb");

        int ret = ps.executeUpdate();
        System.out.println(ret);
      }
    }

    // 查询数据
    try (Connection connection = dataSource.getConnection()) {
      String sql = "select * from " + tableName + " where id=?";
      try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.setString(1, "aa");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
          String id = rs.getString(1);
          String name = rs.getString(2);
          System.out.println("id=" + id);
          System.out.println("name=" + name);
        }
      }
    }

    // 删除数据
    try (Connection connection = dataSource.getConnection()) {
      String sql = "delete from " + tableName + " where id=?";
      try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.setString(1, "aa");
        ps.executeUpdate();
      }
    }
  }
}
