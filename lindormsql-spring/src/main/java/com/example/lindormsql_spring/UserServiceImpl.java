package com.example.lindormsql_spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserServiceImpl implements UserService {
    @Autowired
    private UserDao userDao;

    @Override
    public List<User> getUserList() {
        return userDao.findAll();
    }

    @Override
    public User findUserById(int id) {
        return userDao.findById(id);
    }

    @Override
    public String save(User user) {
        userDao.save(user);
        return "成功添加1条数据";
    }

    @Override
    public String edit(User user) {
        userDao.save(user);
        return "成功更新1条数据";
    }

    @Override
    public String delete(int id) {
        userDao.deleteById(id);
        return "成功删除1条数据";
    }
}
