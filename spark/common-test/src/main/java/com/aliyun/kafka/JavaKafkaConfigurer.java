package com.aliyun.kafka;

import java.io.InputStream;
import java.util.Properties;

public class JavaKafkaConfigurer {

    private static Properties properties;

    public static void configureSasl() {
        //如果用-D 或者其它方式设置过，这里不再设置
        if (null == System.getProperty("java.security.auth.login.config")) {
            //请注意将 XXX 修改为自己的路径
            //这个路径必须是一个文件系统可读的路径，不能被打包到 jar 中
            System.setProperty("java.security.auth.login.config", getKafkaProperties().getProperty("java.security.auth.login.config"));
        }
    }

    public synchronized static Properties getKafkaProperties() {
        if (null != properties) {
            return properties;
        }
        //获取配置文件 kafka.properties 的内容
        Properties kafkaProperties = new Properties();
        try {
            ClassLoader classLoader = KafkaProducerDemoForPublicNet_java.class.getClassLoader();
            InputStream inputStream = classLoader.getResourceAsStream("kafka.properties");
            kafkaProperties.load(inputStream);
            //kafkaProperties.load(KafkaProducerDemoForPublicNet_java.class.getClassLoader().getResourceAsStream("kafka.properties"));
        } catch (Exception e) {
            //没加载到文件，程序要考虑退出
            e.printStackTrace();
        }
        properties = kafkaProperties;
        return kafkaProperties;
    }

}
