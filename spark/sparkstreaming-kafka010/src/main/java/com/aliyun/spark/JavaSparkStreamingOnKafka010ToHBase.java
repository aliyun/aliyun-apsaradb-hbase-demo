package com.aliyun.spark;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
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
 * 实例场景说明：kafka发送字符串，SparkStream获取字符串后按照空格拆分后入库hbase。
 */
public class JavaSparkStreamingOnKafka010ToHBase {

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
    //HBase集群的ZK链接地址。//HBase集群的ZK链接地址。使用时请把此路径替换为你自己的HBase集群的zk访问地址。
    //格式为：xxx-002.hbase.rds.aliyuncs.com:2181,xxx-001.hbase.rds.aliyuncs.com:2181,xxx-003.hbase.rds.aliyuncs.com:2181
    String zkAddress = args[3];
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
    JavaDStream<String[]> words = messages.map(new Function<ConsumerRecord<String, String>, String[]>() {
      @Override
      public String[] call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
        return stringStringConsumerRecord.value().split("\\s+");
      }
    });
    words.foreachRDD(new VoidFunction<JavaRDD<String[]>>() {
      @Override
      public void call(JavaRDD<String[]> javaRDD) throws Exception {
        javaRDD.foreachPartition(new VoidFunction<Iterator<String[]>>() {
          @Override
          public void call(Iterator<String[]> iterator) throws Exception {
            String hbaseTableName = "mytable";
            String cf = "cf1";
            byte[] qualifier1 = "col1".getBytes();
            byte[] qualifier2 = "col2".getBytes();
            Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
            Connection conn = ConnectionFactory.createConnection(hbaseConf);
            Table table = conn.getTable(TableName.valueOf(hbaseTableName));
            List<Put> puts = new ArrayList<Put>();
            int i = 0;
            while (iterator.hasNext()) {
              String[] kv = iterator.next();
              Put put = new Put(kv[0].getBytes());
              put.addColumn(cf.getBytes(), qualifier1, kv[1].getBytes());
              put.addColumn(cf.getBytes(), qualifier2, kv[2].getBytes());
              puts.add(put);
            }
            table.put(puts);
            System.out.println("=====insert into hbase " + i + " rows ===========");
            table.close();
            conn.close();
          }
        });
      }
    });

    //启动SparkStreaming
    ssc.start();

    try {
      ssc.awaitTermination();
    } catch (InterruptedException e) {
    }
  }
}
