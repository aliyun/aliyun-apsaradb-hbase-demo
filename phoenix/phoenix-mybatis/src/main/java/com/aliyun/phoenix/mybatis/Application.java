package com.aliyun.phoenix.mybatis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

/**
 * @author 长轻
 * @date 2019/8/14 4:22 PM
 */
@SpringBootApplication(scanBasePackages = {"com.aliyun.phoenix.mybatis"})
@ServletComponentScan
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
