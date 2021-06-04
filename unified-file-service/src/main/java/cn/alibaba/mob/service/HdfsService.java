/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.alibaba.mob.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class HdfsService {
  private static Logger logger = Logger.getLogger("mob");

  private FileSystem fileSystem;
  
  @Value("${dfs.nameservices}")
  private String dfsNameservices;
  
  @Value("${dfs.root.path}")
  private String rootPath;

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public String getRootPath() {
    return rootPath;
  }

  @PostConstruct
  private void init() {
    Configuration configuration = new Configuration();
    configuration.setBoolean("dfs.client.use.datanode.hostname", true);
    configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    configuration.set("dfs.nameservices", dfsNameservices);
    configuration.set(String.format("dfs.client.failover.proxy.provider.%s", dfsNameservices),
        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    configuration.setBoolean("dfs.ha.automatic-failover.enabled", true);
    configuration.set(String.format("dfs.ha.namenodes.%s", dfsNameservices), "nn1");
    configuration.set(String.format("dfs.namenode.rpc-address.%s.nn1", dfsNameservices),
        String.format("%s-master1-001.lindorm.rds.aliyuncs.com:8020", dfsNameservices));
    configuration.set("fs.defaultFS", "hdfs://" + dfsNameservices);

    try {
      fileSystem = FileSystem.get(configuration);
    } catch (Exception e) {
      logger.error(e);
    }

    logger.warn("hdfs init success...");
  }
  
  
}
