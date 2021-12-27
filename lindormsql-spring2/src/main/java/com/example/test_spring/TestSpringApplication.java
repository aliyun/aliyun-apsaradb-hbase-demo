package com.example.test_spring;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.example.test_spring")
public class TestSpringApplication {
    public static void main(String[] args) {
        SpringApplication.run(TestSpringApplication.class, args);
    }
}
