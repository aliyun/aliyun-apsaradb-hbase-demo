package cn.alibaba.mob;

import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan
@Configuration
@EnableAutoConfiguration
public class TMobApplication extends SpringBootServletInitializer implements ApplicationContextAware {
	
	public static void main(String[] args) {
		SpringApplication.run(TMobApplication.class, args);
	}
	
	@Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
    }

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		return application.sources(TMobApplication.class);
	}
	
}
