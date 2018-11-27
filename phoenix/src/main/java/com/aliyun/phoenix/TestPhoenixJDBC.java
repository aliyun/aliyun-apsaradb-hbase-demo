package com.aliyun.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * create table, create index, insert data, select table.
 */

public class TestPhoenixJDBC {
    public static void main(String[] args) {
        try {
            String zkAddress = args[0];
            Connection con =
                    DriverManager.getConnection("jdbc:phoenix:" + zkAddress);
            Statement stmt = con.createStatement();
            stmt.execute("drop table if exists test");
            stmt.execute("create table test (mykey integer not null primary key, mycolumn varchar)");
            stmt.execute("create index test_idx on test(mycolumn)");
            stmt.executeUpdate("upsert into test values (1,'World!')");
            stmt.executeUpdate("upsert into test values (2,'Hello')");
            stmt.executeUpdate("upsert into test values (3,'World!')");
            con.commit();
            PreparedStatement statement = con.prepareStatement("select mykey from test where mycolumn='Hello'");
            ResultSet rset = statement.executeQuery();
            while (rset.next()) {
                System.out.println(rset.getInt(1));
            }
            stmt.close();
            rset.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
