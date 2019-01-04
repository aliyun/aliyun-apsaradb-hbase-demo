package com.aliyun.phoenix.utils.pool;
/**
 * This file created by mengqingyi on 2019/1/4.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author mengqingyi
 * @classDescription phoenix连接工具类
 * @create 2019年1月4日14:13:44
 **/
@Configuration
public class PhoenixConnectionByJDBC {
    /**
     * 日志
     */
    private static final Logger logger = LoggerFactory.getLogger(PhoenixConnectionByJDBC.class);
    /**
     * phoenix ZK 地址(VPC)
     */
    private static final String URL = "jdbc:phoenix:XXX";

    /**
     * 由于基础的HBase连接，Phoenix的Connection对象与大多数其他JDBC Connections不同。 Phoenix
     * Connection对象被设计为一个成本低廉的薄对象。如果PhoenixConnections被重新使用，
     * 那么基本的HBase连接可能并不总是被前一个用户保持在健康状态。最好创建新的Phoenix Connections，以确保避免任何潜在的问题。
     * 通过创建一个委托连接，可以通过创建一个代理连接，从池中检索到新的Phoenix连接，然后在将其返回到池时关闭连接
     */
    public static Connection getConnection() {
        Connection connection = null;
        if (connection == null) {
            try {
                logger.info("phoinx地址：{}", URL);
                Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                connection = DriverManager.getConnection(URL);
            } catch (Exception e) {
                logger.error("[hbase]获取连接异常!!" + e.getMessage());
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * 依次关闭资源
     *
     * @param connection 连接
     * @param statement  准备语句
     * @param resultSet  结果集
     */
    public static Boolean close(Connection connection, PreparedStatement statement, ResultSet resultSet) {
        Boolean flag = Boolean.TRUE;
        if (null != resultSet) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                flag = Boolean.FALSE;
                logger.error("[PhoenixConnectionByJDBC]结果集rs关闭异常");
                e.printStackTrace();
            }
        }

        if (null != statement) {
            try {
                statement.close();
            } catch (SQLException e) {
                flag = Boolean.FALSE;
                logger.error("[PhoenixConnectionByJDBC]准备语句pstmt关闭异常");
                e.printStackTrace();
            }
        }
        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e) {
                flag = Boolean.FALSE;
                logger.error("[PhoenixConnectionByJDBC]连接conn关闭异常");
                e.printStackTrace();
            }
        }
        return flag;
    }

    /**
     * ddl 关闭资源
     *
     * @param connection 连接
     * @param statement  语句
     */
    public static Boolean close(Connection connection, Statement statement) {
        Boolean flag = Boolean.TRUE;
        if (null != statement) {
            try {
                statement.close();
            } catch (SQLException e) {
                flag = Boolean.FALSE;
                logger.error("[PhoenixConnectionByJDBC]准备语句pstmt关闭异常");
                e.printStackTrace();
            }
        }
        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e) {
                flag = Boolean.FALSE;
                logger.error("[PhoenixConnectionByJDBC]连接conn关闭异常");
                e.printStackTrace();
            }
        }
        return flag;
    }
}
