package com.aliyun.phoenix;

import java.sql.*;

/**
 * 使用轻客户端对phoenix的基本操作：创建表，建索引，批量写数据，查询
 */
public class TestPhoenix {

    public static void main(String[] args) {
        try {
            // QueryServer连接地址替换为运行环境地址
            String queryServerAddress = "http://localhost:8765";
            Connection conn = PhoenixThinClientUtil.getConnection(queryServerAddress);
            conn.setAutoCommit(true);

            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists test");

            // 创建测试表
            stmt.execute("create table test (mykey integer not null primary key, mycolumn varchar, mytime timestamp) salt_buckets = 4");

            // 创建测试表索引
            stmt.execute("create index test_idx on test(mycolumn) include (mytime)");

            // 使用PreparedStatement插入数据, executeBatch可以减少rpc次数从而提高写入性能
            PreparedStatement pstmt = conn.prepareStatement("upsert into test(mykey, mycolumn, mytime) values(?, ?, ?)");
            pstmt.setInt(1, 1);
            pstmt.setString(2, "World!");
            pstmt.setTimestamp(3, Timestamp.valueOf("2018-12-11 10:10:10"));
            pstmt.addBatch();

            pstmt.setInt(1, 2);
            pstmt.setString(2, "Hello");
            pstmt.setTimestamp(3, Timestamp.valueOf("2018-12-11 10:10:20"));
            pstmt.addBatch();

            pstmt.setInt(1, 3);
            pstmt.setString(2, "World!");
            pstmt.setTimestamp(3, Timestamp.valueOf("2018-12-11 10:10:30"));
            pstmt.addBatch();

            pstmt.executeBatch();

            // 查询数据
            ResultSet rset = stmt.executeQuery("select * from test");
            while (rset.next()) {
                System.out.println(rset.getInt(1) + " : " + rset.getString(2) + " : " + rset.getTimestamp(3));
            }
            // 使用索引列过滤查询
            rset = stmt.executeQuery("select mykey, mytime from test where mycolumn = 'Hello'");
            while (rset.next()) {
                System.out.println(rset.getInt(1) + " : " + rset.getTimestamp(2));
            }

            rset.close();
            pstmt.close();
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
