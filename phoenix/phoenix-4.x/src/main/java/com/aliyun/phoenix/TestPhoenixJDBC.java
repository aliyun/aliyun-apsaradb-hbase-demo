package com.aliyun.phoenix;

import java.sql.*;
import java.util.Properties;

/**
 * create table, create index, insert data, select table.
 */

public class TestPhoenixJDBC {
    public static void main(String[] args) {
        try {
            String zkAddress = args[0];
            Properties p = new Properties();
            p.put("phoenix.query.dateFormatTimeZone", "GMT+8");
            Connection con =
                    DriverManager.getConnection("jdbc:phoenix:" + zkAddress, p);
            Statement stmt = con.createStatement();
            stmt.execute("create table if not exists test (mykey integer not null primary key, mycolumn varchar, mytime timestamp) salt_buckets = 4");
            stmt.execute("create index if not exists test_idx on test(mycolumn) include (mytime)");
            stmt.close();
            con.commit();

            PreparedStatement pstmt = con.prepareStatement("upsert into test(mykey, mycolumn, mytime) values(?, ?, ?)");

            pstmt.setInt(1, 1);
            pstmt.setString(2, "World!");
            pstmt.setTimestamp(3, Timestamp.valueOf("2018-12-11 10:10:10"));
            pstmt.executeUpdate();

            pstmt.setInt(1, 2);
            pstmt.setString(2, "Hello");
            pstmt.setTimestamp(3, Timestamp.valueOf("2018-12-11 10:10:20"));
            pstmt.executeUpdate();

            pstmt.setInt(1, 3);
            pstmt.setString(2, "World!");
            pstmt.setTimestamp(3, Timestamp.valueOf("2018-12-11 10:10:30"));
            pstmt.executeUpdate();

            con.commit();

            ResultSet rset = pstmt.executeQuery("select mykey, mytime from test where mycolumn='Hello'");
            while (rset.next()) {
                System.out.println(rset.getInt(1) + " : " + rset.getTimestamp(2));
            }
            rset.close();
            pstmt.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
