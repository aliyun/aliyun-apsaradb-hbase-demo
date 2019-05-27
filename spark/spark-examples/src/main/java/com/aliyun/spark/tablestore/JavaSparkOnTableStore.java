/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.spark.tablestore;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.tablestore.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

public class JavaSparkOnTableStore {
    private static String endpoint;
    private static String accessKeyId;
    private static String accessKeySecret;
    private static String securityToken;
    private static String instance;
    private static String table;

    private static SyncClient getOTSClient() {
        Credential cred = new Credential(accessKeyId, accessKeySecret, securityToken);
        Endpoint ep;
        if (instance == null) {
            ep = new Endpoint(endpoint);
        } else {
            ep = new Endpoint(endpoint, instance);
        }
        if (cred.securityToken == null) {
            return new SyncClient(ep.endpoint, cred.accessKeyId,
                cred.accessKeySecret, ep.instance);
        } else {
            return new SyncClient(ep.endpoint, cred.accessKeyId,
                cred.accessKeySecret, ep.instance, cred.securityToken);
        }
    }

    private static TableMeta fetchTableMeta() {
        SyncClient ots = getOTSClient();
        try {
            DescribeTableResponse resp = ots.describeTable(
                new DescribeTableRequest(table));
            return resp.getTableMeta();
        } finally {
            ots.shutdown();
        }
    }

    private static RangeRowQueryCriteria fetchCriteria() {
        TableMeta meta = fetchTableMeta();
        RangeRowQueryCriteria res = new RangeRowQueryCriteria(table);
        res.setMaxVersions(1);
        List<PrimaryKeyColumn> lower = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> upper = new ArrayList<PrimaryKeyColumn>();
        for(PrimaryKeySchema schema: meta.getPrimaryKeyList()) {
            lower.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MIN));
            upper.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MAX));
        }
        res.setInclusiveStartPrimaryKey(new PrimaryKey(lower));
        res.setExclusiveEndPrimaryKey(new PrimaryKey(upper));
        return res;
    }

    public static void main(String[] args) {

        endpoint = args[0];
        accessKeyId = args[1];
        accessKeySecret = args[2];
        instance = args[3];
        table = args[4];
        if(args.length > 5) {
            securityToken = args[5];
        }

        System.out.println("endpoint=" + endpoint);
        System.out.println("accessKeyId=" + accessKeyId);
        System.out.println("accessKeySecret=" + accessKeySecret);
        System.out.println("table=" + table);

        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkOnTableStore");
        JavaSparkContext sc = null;
        try {
            sc = new JavaSparkContext(sparkConf);
            Configuration hadoopConf = new Configuration();
            TableStore.setCredential(
                hadoopConf,
                new Credential(accessKeyId, accessKeySecret, securityToken));
            Endpoint ep = new Endpoint(endpoint, instance);
            TableStore.setEndpoint(hadoopConf, ep);
            TableStoreInputFormat.addCriteria(hadoopConf, fetchCriteria());

            JavaPairRDD<PrimaryKeyWritable, RowWritable> rdd = sc.newAPIHadoopRDD(
                hadoopConf, TableStoreInputFormat.class, PrimaryKeyWritable.class,
                RowWritable.class);
            System.out.println(
                new Formatter().format("TOTAL: %d", rdd.count()).toString());
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }
}
