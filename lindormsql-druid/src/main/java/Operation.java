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


import java.sql.*;
import java.util.Random;

public class Operation {

    private Connection pconn;
    private String tableName = null;

    public Operation(Connection conn) {
        pconn = conn;
    }

    private void closeJDBCConnection() throws Exception {
        if (null != pconn) {
            pconn.close();
        }
    }

    public void runSql() throws Exception {
        createTable();
        upsert();
        select();
        delete();
        select();
        dropTable();
        closeJDBCConnection();
    }

    public void createTable() throws SQLException {
        tableName = "sql_table_" + new Random().nextInt(1000);
        String sql = "create table if not exists " + tableName
                + "(id VARCHAR, name VARCHAR, primary key(id))";
        System.out.println(sql);
        Statement statement = null;
        try {
            statement = pconn.createStatement();
            int ret = statement.executeUpdate(sql);
            System.out.println(ret);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }


    public void upsert() throws SQLException {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("upsert into " + tableName + "(id,name) values(?,?)");
        String sql = sqlBuilder.toString();

        PreparedStatement ps = null;
        try {
            ps = pconn.prepareStatement(sql);
            ps.setString(1, "aa");
            ps.setString(2, "bb");

            int ret = ps.executeUpdate();
            System.out.println(ret);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    public void select() throws SQLException {
        String sql = "select * from " + tableName + " where id=?";
        System.out.println(sql);
        PreparedStatement ps = null;
        try {
            ps = pconn.prepareStatement(sql);
            ps.setString(1, "aa");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String id = rs.getString(1);
                String name = rs.getString(2);

                System.out.println("id=" + id);
                System.out.println("name=" + name);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    public void delete() throws SQLException {
        String sql = "delete from " + tableName + " where id=?";
        System.out.println(sql);
        PreparedStatement ps = null;
        try {
            ps = pconn.prepareStatement(sql);
            ps.setString(1, "aa");

            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    // Lindorm 2.2.16版本后,在执行删除表操作之前需要执行OFFLINE TABLE操作。
    public void dropTable() throws SQLException {
        String sql = "drop table " + tableName;
        System.out.println(sql);
        Statement statement = null;
        try {
            statement = pconn.createStatement();
            int ret = statement.executeUpdate(sql);
            System.out.println(ret);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }
}
