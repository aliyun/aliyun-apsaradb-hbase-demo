import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Properties;

public class Demo {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        InputStream inputStream = Demo.class.getClassLoader().getResourceAsStream("druid.properties");
        properties.load(inputStream);
        DataSource ds = DruidDataSourceFactory.createDataSource(properties);
        Connection conn = ds.getConnection();
        System.out.println(conn);
        Operation op = new Operation(conn);
        op.runSql();
    }
}
