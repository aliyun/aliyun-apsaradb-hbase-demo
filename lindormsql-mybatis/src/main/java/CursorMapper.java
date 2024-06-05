import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;
public interface CursorMapper {
  @Update("create table if not exists cursor_tbl(id INT, record VARCHAR, primary key(id))")
  void createTable();

  @Update("drop table if exists cursor_tbl")
  void dropTable();

  @Insert("insert into cursor_tbl(id, record) values(#{id}, #{record})")
  void insertCursor(Cursor cursor);

  @Select("select /*+ _l_cursor_ */ id, record from cursor_tbl limit ${offset}, ${numRows}")
  List<Cursor> selectWithoutCursor(@Param("offset") int offset, @Param("numRows") int numRows);

  // 引用变量使用${Variable}格式，声明变量要用@Param("Variable")格式
  @Select("select /*+ _l_cursor_('" + "${next}" + "') */ id, record from cursor_tbl limit ${offset}, ${numRows}")
  List<Cursor> selectWithCursor(@Param("next") String next, @Param("offset") int offset, @Param("numRows") int numRows);
}
