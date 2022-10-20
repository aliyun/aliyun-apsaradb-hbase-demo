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

import com.aliyun.lindorm.pool.LindormDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

/**
 * @author jianhong.hjh
 */
public class LindormSQLPoolDemo2 {
  public static void main(String[] args) throws Exception {
    /* -------------- 初始化参数 ----------------- */

    // 指定驱动类名, 需要根据引擎类型指定对应的驱动名称, 这里以宽表引擎作为示例
    //   * 宽表引擎驱动类名: com.aliyun.lindorm.table.client.Driver
    //   * 时序引擎驱动类名: com.aliyun.lindorm.tsdb.client.Driver
    //   * 搜索引擎驱动类名: com.aliyun.lindorm.search.client.Driver
    String driverClassName = "com.aliyun.lindorm.table.client.Driver";

    // url、username、password需要替换为业务实际的内容
    // 实际的url、username、password可以在实例控制台上获取
    String url = "jdbc:lindorm:table:url=http://xxxxxxx-proxy-lindorm.lindorm.rds.aliyuncs.com:30060";
    String username = "********";
    String password = "********";
    // 连接属性，指定要连接的database，需根据实际情况将database=xxx, 替换为实际的内容
    String connectionProperties = "database=xxx";

    // 连接初始数，可以根据实际情况调整
    int initialSize = 10;
    // 连接最大idle数，可以根据实际情况调整
    int minIdle = 10;
    // 连接最大数，可以根据实际情况调整
    int maxActive = 20;

    /* -------------- 初始化连接池 ----------------- */
    LindormDataSource dataSource = new LindormDataSource();
    dataSource.setDriverClassName(driverClassName);
    dataSource.setUrl(url);
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    dataSource.setInitialSize(initialSize);
    dataSource.setMinIdle(minIdle);
    dataSource.setMaxActive(maxActive);
    // 当宽表引擎版本低于2.3.1时，取消下行的注释
    // dataSource.setValidationQuery("SHOW DATABASES");
    dataSource.setConnectionProperties(connectionProperties);


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
