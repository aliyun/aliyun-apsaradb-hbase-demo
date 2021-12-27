package com.example.test_spring;

import org.springframework.stereotype.Service;

import java.util.List;

@Service
public interface UserService {

    List<User> getUserList();
    String AddUser(User user);

}