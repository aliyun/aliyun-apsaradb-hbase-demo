package com.aliyun.spark;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.*;

/**
 * 此demo为SparkStreaming使用kafka010客户的实例，适用于阿里云互联网中间件->消息队列kafka。
 * 实例场景说明：kafka发送字符串，SparkStream获取字符串后按照空格拆分成单词，然后记录每个单词的总个数。
 * 运行命令：
 * spark-submit --master yarn --class com.aliyun.spark.JavaSparkStreamingOnKafka010 /opt/jars/test/sparkstreaming-kafka010-0.0.1-SNAPSHOT.jar
 * ip1:9092,ip2:9092,ip3:9092 topic groupId
 */
public class JavaSparkStreamingOnKafka010 {

  public static void main(String[] args) {
    // kafka的broker，在阿里云互联网中间件->消息队列kafka中，topic管理或者 Consumer Group管理中的"获取接入点"中获取。格式为：ip:port,ip:port,ip:port
    String brokers = args[0];
    // kafka的 topic，在阿里云互联网中间件->消息队列kafka中，topic管理中创建。
    String topic = args[1];
    // kafka GroupID，可从kafka服务的Consumer Group 管理获取。
    String groupId = args[2];
    // SparkStreaming 批处理的时间间隔
    int batchSize = 10;
    SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new
            Duration(batchSize * 1000));
    // 添加topic。
    List<String> topicList = new ArrayList<>();
    topicList.add(topic);
    //设置kafka参数
    Map<String, Object> kafkaParams = new HashMap();
    kafkaParams.put("bootstrap.servers", brokers);
    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    //SparkStreaming 的executor消费kafka数据会创建另外一个groupID，格式为：spark-executor-${groupId}.
    //如果使用阿里云互联网中间件->消息队列kafka，需要前往阿里云互联网中间件->消息队列kafka中，
    // 在Consumer Group管理中创建一个名词为spark-executor-${groupId}的group。
    kafkaParams.put("group.id", groupId);
    LocationStrategy locationStrategy = LocationStrategies.PreferConsistent();
    ConsumerStrategy consumerStrategy = ConsumerStrategies.Subscribe(topicList, kafkaParams);

    //从Kafka接收数据并创建对应的DStream。
    JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(ssc,
            locationStrategy, consumerStrategy);

    //对获取的数据按照空格拆分单词
    JavaDStream<String> words = messages.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
      @Override
      public Iterator<String> call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
        String value = stringStringConsumerRecord.value();
        return Lists.newArrayList(value.split(" ")).iterator();
      }
    });

    //对单词计总数
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
            new PairFunction<String, String, Integer>() {
              @Override
              public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
              }
            }
    ).reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
      }
    }).updateStateByKey(
            new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
              @Override
              public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
                int out = 0;
                if (state.isPresent()) {
                  out += state.get();
                }
                for (Integer v : values) {
                  out += v;
                }
                return Optional.of(out);
              }
            });

    //打印单词总个数。
    wordCounts.print();
    //启动SparkStreaming
    ssc.start();

    try {
      ssc.awaitTermination();
    } catch (InterruptedException e) {
    }
  }
}
