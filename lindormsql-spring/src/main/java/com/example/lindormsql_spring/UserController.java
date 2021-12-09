package com.example.lindormsql_spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class UserController {
    @Autowired
    private UserServiceImpl userServer;

    @RequestMapping("/list")
    public List<User> list() {
        return userServer.getUserList();
    }

    @RequestMapping("/select")
    public User select(int id) {
        return userServer.findUserById(id);
    }

    @RequestMapping("/add")
    public String add(User user) {
        return userServer.save(user);
    }

    @RequestMapping("/update")
    public String update(User user) {
        return userServer.edit(user);
    }

    @RequestMapping("/delete")
    public String delete(int id) {
        return userServer.delete(id);
    }
}
