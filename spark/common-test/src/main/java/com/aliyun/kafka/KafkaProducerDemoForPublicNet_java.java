package com.aliyun.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class KafkaProducerDemoForPublicNet_java {

    public static void main(String[] args) {
        //设置 sasl 文件的路径
        JavaKafkaConfigurer.configureSasl();

        //加载 kafka.properties
        Properties kafkaProperties =  JavaKafkaConfigurer.getKafkaProperties();

        Properties props = new Properties();
        //设置接入点，即控制台的实例详情页显示的“SSL接入点”
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("bootstrap.servers"));
        //设置 SSL 根证书的路径，请记得将 XXX 修改为自己的路径
        //与 sasl 路径类似，该文件也不能被打包到 jar 中
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, kafkaProperties.getProperty("ssl.truststore.location"));
        //根证书 store 的密码，保持不变
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "KafkaOnsClient");
        //接入协议，目前支持使用 SASL_SSL 协议接入
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        //SASL 鉴权方式，保持不变
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        //Kafka 消息的序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //请求的最长等待时间
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30 * 1000);

        //构造 Producer 对象，注意，该对象是线程安全的，一般来说，一个进程内一个 Producer 对象即可；
        //如果想提高性能，可以多构造几个对象，但不要太多，最好不要超过 5 个
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //构造一个 Kafka 消息
        String topic = kafkaProperties.getProperty("topic"); //消息所属的 Topic，请在控制台申请之后，填写在这里
        long sleepTime = Long.valueOf(kafkaProperties.getProperty("record.generate.sleep.time", "2000"));

        //消息的内容的表结构为：
        //state string
        //city string
        //population int
        int start = Integer.valueOf(kafkaProperties.getProperty("record.generate.start.offset", "0"));
        String state, city;
        int population;
        char[] chars = new char[26];
        for (int i=65; i<=90; i++) {
            chars[i-65] = (char)i;
        }
        Random random = new Random();
        try {
            while (start < Integer.MAX_VALUE) {

                state = String.valueOf(chars[random.nextInt(26)]) + chars[random.nextInt(26)];
                city = "city_" + String.format("%09d", start);
                population = random.nextInt(20000000);
                String value = state + "," + city + "," + population;
                ProducerRecord<String, String> kafkaMessage =  new ProducerRecord<String, String>(topic, value);
                //发送消息，并获得一个 Future 对象
                Future<RecordMetadata> metadataFuture = producer.send(kafkaMessage);
                //同步获得 Future 对象的结果
                RecordMetadata recordMetadata = metadataFuture.get();
                System.out.println("Produce ok, values: " + value + " return info:" + recordMetadata.toString());
                Thread.sleep(sleepTime);
                start++;
            }

        } catch (Exception e) {
            //要考虑重试，参见常见问题: https://help.aliyun.com/document_detail/68168.html
            System.out.println("error occurred");
            e.printStackTrace();
        }

    }
}
