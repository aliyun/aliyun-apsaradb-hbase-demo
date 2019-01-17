package com.aliyun.phoenix;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * 使用phoenix轻客户端多线程写入数据，分页查询示例
 */
public class TestPhoenixPageQuery {

    private static String DROP_TABLE_TEMPLATE = "DROP TABLE IF EXISTS %s";
    private static String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s ("
                                                            + "A VARCHAR NOT NULL PRIMARY KEY,"
                                                            + "B VARCHAR,"
                                                            + "C VARCHAR,"
                                                            + "D VARCHAR,"
                                                            + "E VARCHAR,"
                                                            + "F VARCHAR,"
                                                            + "G VARCHAR) SALT_BUCKETS=4";
    private static String CREATE_INDEX_TEMPLATE = "CREATE INDEX %s on %s (C,D,E) INCLUDE (F, G)";
    private static String UPSERT_TABLE_TEMPLATE = "UPSERT INTO %s (A,B,C,D,E,F,G) VALUES (?,?,?,?,?,?,?)";
    private static String COUNT_TABLE_TEMPLATE = "SELECT count(*) FROM %s";
    private static String PAGE_QUERY_TEMPLATE = "SELECT * FROM %s limit ? offset ?";

    private static String tableName = "PAGE_QUERY_TEST";
    private static String indexName = "PAGE_QUERY_IDX";
    private static final int columns = 7;
    private static int numRecords = 10000;
    private static int nThreads = 10;
    private static final int batchSize = 100;

    public static void main(String[] args) {
        try {
            // QueryServer连接地址替换为运行环境地址
            String queryServerAddress = "http://localhost:8765";

            // 创建表和索引
            createTableAndIndex(queryServerAddress);
            // 多线程写入测试数据
            writeData(queryServerAddress);
            // 统计数据条数
            countData(queryServerAddress);
            // 分页查询打印
            pageQuery(queryServerAddress);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createTableAndIndex(String queryServerAddress) throws SQLException {
        Connection conn = PhoenixThinClientUtil.getConnection(queryServerAddress);
        Statement statement = conn.createStatement();

        statement.execute(String.format(DROP_TABLE_TEMPLATE, tableName));
        // create table
        System.out.println("Create table ...");
        statement.execute(String.format(CREATE_TABLE_TEMPLATE, tableName));

        System.out.println("Create index ...");
        statement.execute(String.format(CREATE_INDEX_TEMPLATE, indexName, tableName));

        statement.close();
        conn.close();
    }

    public static void writeData(final String queryServerAddress) throws Exception {
        ExecutorService threadPool = Executors.newFixedThreadPool(nThreads);
        List<Future<String>> tasks = new ArrayList<Future<String>>();

        for (int n = 0; n < nThreads; n++) {

            final String threadName = "Client-" + n;
            tasks.add(threadPool.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    Connection conn = PhoenixThinClientUtil.getConnection(queryServerAddress);
                    conn.setAutoCommit(true);
                    PreparedStatement pstmt = conn.prepareStatement(String.format(UPSERT_TABLE_TEMPLATE, tableName));

                    for (int i = 0; i < numRecords / nThreads; i++) {
                        for (int j = 1; j <= columns; j++) {
                            Random random = new Random();
                            String v =  String.valueOf(random.nextInt(numRecords)) + "_" + threadName + i;
                            pstmt.setString(j, v);
                        }
                        pstmt.addBatch();

                        if (i % batchSize == 0)
                        {
                            pstmt.executeBatch();
                            pstmt.clearBatch();
                        }

                    }
                    pstmt.executeBatch();

                    pstmt.close();
                    conn.close();

                    return threadName;
                }
            }));

        }

        for (Future task : tasks) {
            System.out.println(task.get() + " finished ...");

        }

        threadPool.shutdown();
        threadPool.awaitTermination(30, TimeUnit.SECONDS);


    }

    public static void countData(String queryServerAddress) throws SQLException {
        Connection conn = PhoenixThinClientUtil.getConnection(queryServerAddress);
        Statement st = conn.createStatement();
        String countSql = String.format(COUNT_TABLE_TEMPLATE, tableName);
        ResultSet resultSet = st.executeQuery(countSql);
        if (resultSet.next()) {
            System.out.println(tableName + " count: " + resultSet.getInt(1));
        }
        st.close();
        conn.close();
    }

    public static void pageQuery(String queryServerAddress) throws SQLException {
        Connection conn = PhoenixThinClientUtil.getConnection(queryServerAddress);
        String pageQuerySQL = String.format(PAGE_QUERY_TEMPLATE, tableName);

        PreparedStatement pstmt = conn.prepareStatement(pageQuerySQL);
        // query 10 record per page
        int limit = 10;
        // query data from offset, query 10 page
        int offset = 0;
        for (int page = 0; page < 10; page ++) {
            offset = page*limit;
            pstmt.setInt(1, limit);
            pstmt.setInt(2, offset);

            ResultSet resultSet = pstmt.executeQuery();

            while (resultSet.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 0; i < columns; i++) {
                    line.append(resultSet.getString(1)).append(", ");
                }
                System.out.println("Page " + page + ": " + line.toString());
            }
        }
    }

}
