package com.example.test_spring;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface UserMapper {

    @Select("select id,name from user_test")
    List<User> getUserList();
    @Insert("insert into user_test(id,name) values(#{id},#{name})")
    int AddUser(User user);
}