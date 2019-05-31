package com.aliyun.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class KafkaProducerDemoForTableData_java {

    public static void main(String[] args) {
        String bootstrapServers = args[0];
        String topic = args[1]; //消息所属的 Topic，请在控制台创建后，填写在这里
        long sleepTime = 2000;
        if (args.length > 2) {
            sleepTime = Long.valueOf(args[2]);
        }

        Properties props = new Properties();
        //设置接入点，请通过控制台获取对应 Topic 的接入点
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //消息队列 Kafka 消息的序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //请求的最长等待时间
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30 * 1000);
        //构造 Producer 对象，注意，该对象是线程安全的。
        //一般来说，一个进程内一个 Producer 对象即可。如果想提高性能，可构造多个对象，但最好不要超过 5 个
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        //构造一个消息队列 Kafka 消息

        //消息的内容的表结构为：
        //state string
        //city string
        //population int
        int start = 0;
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
