import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

public interface UserMapper {
    @Select("select * from demo_user where userId=#{id}")
    User selectOneUser(String id);

    @Select("select * from demo_user")
    List<User> selectAllUser();

    @Update("create table if not exists demo_user(userId VARCHAR, userName VARCHAR, primary key(userId))")
    void createUser();

    @Update("drop table if exists demo_user")
    void dropUser();

    @Insert("insert into demo_user(userId,userName) values(#{userId},#{userName})")
    int insertUser(User user);
}
