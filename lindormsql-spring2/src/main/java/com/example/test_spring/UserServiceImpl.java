package com.example.test_spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class UserServiceImpl implements UserService {
    @Autowired
    private UserMapper userMapper;
    @Override
    public List<User> getUserList() {

        try {
            List<User> users = userMapper.getUserList();

            return  users;
        }
        catch (Exception e)
        {
            throw e;
//            return null;
        }
    }
    @Override
    public String AddUser(User user) {
        try {

            int i = userMapper.AddUser(user);
            return "添加成功" + i + "条数据";
        }
        catch (Exception e)
        {
            throw e;
        }
    }
}