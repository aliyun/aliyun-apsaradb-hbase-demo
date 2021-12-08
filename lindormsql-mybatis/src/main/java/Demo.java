import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;

public class Demo {
    public static void main(String[] args) throws IOException {
        String resource = "mybatis-config.xml";
        InputStream inputStream = Resources.getResourceAsStream(resource);
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        try (SqlSession session = sqlSessionFactory.openSession()) {
            UserMapper mapper = session.getMapper(UserMapper.class);
            mapper.createUser();
            session.commit();
            System.out.println("create table user");
            System.out.println(mapper.selectAllUser());
            mapper.insertUser(new User("001", "zhangsan"));
            mapper.insertUser(new User("002", "lisi"));
            session.commit();
            System.out.println(mapper.selectAllUser());
            System.out.println(mapper.selectOneUser("001"));
            mapper.dropUser();  //Lindorm 2.2.16版本后,在执行删除表操作之前需要执行OFFLINE TABLE操作。
            session.commit();
            System.out.println("drop table user");
        }
    }
}
