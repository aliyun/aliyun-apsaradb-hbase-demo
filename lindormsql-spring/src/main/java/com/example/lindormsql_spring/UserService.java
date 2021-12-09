package com.example.lindormsql_spring;

import java.util.List;

public interface UserService {
    List<User> getUserList();

    User findUserById(int id);

    String save(User user);

    String edit(User user);

    String delete(int id);
}
