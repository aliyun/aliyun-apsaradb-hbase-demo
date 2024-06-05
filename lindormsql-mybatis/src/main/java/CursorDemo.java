import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class CursorDemo {
  public static void main(String[] args) throws IOException {
    String resource = "mybatis-config-cursor.xml";
    InputStream inputStream = Resources.getResourceAsStream(resource);
    SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
    try (SqlSession session = sqlSessionFactory.openSession()) {
      CursorMapper mapper = session.getMapper(CursorMapper.class);
      System.out.println("create table cursor_tbl");
      mapper.createTable();
      session.commit();

      System.out.println("insert 20 rows into cursor_tbl");
      for (int i = 1; i <= 20; i++) {
        mapper.insertCursor(new Cursor(i, "v" + i));
      }
      session.commit();

      System.out.println("select first 5 rows");
      List<Cursor> firstFive = mapper.selectWithoutCursor(0, 5);
      firstFive.forEach(System.out::println);

      System.out.println("select second 5 rows");
      String next = firstFive.get(0).getNext();
      List<Cursor> secondFive = mapper.selectWithCursor(next, 5, 5);
      secondFive.forEach(System.out::println);

      System.out.println("select third 5 rows");
      next = secondFive.get(0).getNext();
      List<Cursor> thirdFive = mapper.selectWithCursor(next, 10, 5);
      thirdFive.forEach(System.out::println);

      System.out.println("select fourth 5 rows");
      next = thirdFive.get(0).getNext();
      List<Cursor> fourthFive = mapper.selectWithCursor(next, 15, 5);
      fourthFive.forEach(System.out::println);

      mapper.dropTable();  //Lindorm 2.2.16版本后,在执行删除表操作之前需要执行OFFLINE TABLE操作。
      session.commit();
      System.out.println("drop table cursor_tbl");
    }
  }
}
