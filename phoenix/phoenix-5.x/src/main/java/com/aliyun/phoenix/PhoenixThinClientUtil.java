package com.aliyun.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Phoenix轻客户端工具类
 */
public class PhoenixThinClientUtil {
    /**
     * 轻客户端URL驱动类名
     */
    private static final String DRIVER_CLASS_STRING = "org.apache.phoenix.queryserver.client.Driver";
    /**
     * 轻客户端URL前缀
     */
    private static final String CONNECT_STRING_PREFIX = "jdbc:phoenix:thin:";
    /**
     * 缺省序列化协议
     */
    private static final String DEFAULT_SERIALIZATION = "PROTOBUF";

    public static Connection getConnection(String queryServerAddress) {
        return getConnection(queryServerAddress, DEFAULT_SERIALIZATION);
    }

    public static Connection getConnection(String queryServerAddress, String serialization) {
        String url = getConnectionUrl(queryServerAddress, serialization);
        try {
            Class.forName(DRIVER_CLASS_STRING);
            return DriverManager.getConnection(url);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Load driver class [" + DRIVER_CLASS_STRING + "] failed!" ,e);
        } catch (SQLException sqle) {
            throw new RuntimeException("Create phoenix thin client connection failed, check queryserver state please.", sqle);
        }
    }

    public static Connection getConnection(String queryServerAddress, Properties properties) {
        return getConnection(queryServerAddress, DEFAULT_SERIALIZATION, properties);
    }

    public static Connection getConnection(String queryServerAddress, String serialization, Properties properties) {
        String url = getConnectionUrl(queryServerAddress, serialization);
        try {
            Class.forName(DRIVER_CLASS_STRING);
            return DriverManager.getConnection(url, properties);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Load driver class [" + DRIVER_CLASS_STRING + "] failed!" ,e);
        } catch (SQLException sqle) {
            throw new RuntimeException("Create phoenix thin client connection failed, check queryserver state please.", sqle);
        }
    }

    public static String getConnectionUrl(String queryServerAddress, String serialization) {
        String urlFmt = CONNECT_STRING_PREFIX + "url=%s;serialization=%s";
        return String.format(urlFmt, queryServerAddress, serialization);
    }
}
