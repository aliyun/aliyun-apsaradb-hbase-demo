package cn.alibaba.mob.service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


@Service
public class HbaseService {

  private static Logger logger = Logger.getLogger("mob");

  @Value("${hbase.pool.size}")
  private int poolSize;

  @Value("${hbase.zookeeper.quorum}")
  private String zkAddr;

  @Value("${hbase.client.username}")
  private String userName;
  @Value("${hbase.client.password}")
  private String passwd;
  @Value("${lindorm.rpc.only.use.seedserver}")
  private Boolean onlySeedserver;
  @Value("${hbase.client.connection.impl}")
  private String connectionImpl;
  
  @Value("${hbase.namespace}")
  private String hbaseNs;

  private Connection conn;

  public Connection getConn() {
    return conn;
  }

  public String decorateTable(final String tableName) {
    return this.hbaseNs + ":" + tableName;
  }
  
  @PostConstruct public void init() {
    Configuration conf = HBaseConfiguration.create();
    
    conf.set("hbase.client.ipc.pool.size", String.valueOf(poolSize));
    conf.set("hbase.zookeeper.quorum", zkAddr);
    conf.set("hbase.client.username", userName);
    conf.set("hbase.client.password", passwd);
    conf.setBoolean("lindorm.rpc.only.use.seedserver", onlySeedserver);
    conf.set("hbase.client.connection.impl", connectionImpl);
    

    try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (Exception e) {
      logger.error(e);
    }

    logger.warn("hbase init success...");
  }

  @PreDestroy public void close() {
    if (conn != null) {
      IOUtils.closeQuietly(conn);
    }
    logger.warn("hbase close success...");
  }

}
