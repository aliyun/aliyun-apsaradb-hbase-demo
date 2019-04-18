package com.aliyun.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerDemo_java {

    public static void main(String[] args) {
        String bootstrapServers = args[0];
        String topic = args[1]; //消息所属的 Topic，请在控制台创建后，填写在这里

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

        String value = "this is the message's value"; //消息的内容
        ProducerRecord<String, String> kafkaMessage =  new ProducerRecord<String, String>(topic, value);
        try {
            while (true) {
                //发送消息，并获得一个 Future 对象
                Future<RecordMetadata> metadataFuture = producer.send(kafkaMessage);
                //同步获得 Future 对象的结果
                RecordMetadata recordMetadata = metadataFuture.get();
                System.out.println("Produce ok:" + recordMetadata.toString());
                Thread.sleep(2000);
            }

        } catch (Exception e) {
            //要考虑重试，参见常见问题: https://help.aliyun.com/document_detail/68168.html
            System.out.println("error occurred");
            e.printStackTrace();
        }
    }
}
