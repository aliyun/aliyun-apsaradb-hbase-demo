/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.lindorm.sql.demo;

import com.aliyun.lindorm.pool.LindormDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

/**
 *
 * @author jianhong.hjh
 */
public class LindormSQLPoolDemo {
  public static void main(String[] args) throws Exception{
    // 加载参数
    Properties properties = new Properties();
    InputStream inputStream = LindormSQLPoolDemo.class.getClassLoader().getResourceAsStream("pool.properties");
    properties.load(inputStream);
    // 初始化连接池
    DataSource dataSource = LindormDataSourceFactory.createDataSource(properties);


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
