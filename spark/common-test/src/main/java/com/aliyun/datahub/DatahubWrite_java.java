package com.aliyun.datahub;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.Account;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.ErrorEntry;
import com.aliyun.datahub.model.GetProjectResult;
import com.aliyun.datahub.model.PutRecordsResult;
import com.aliyun.datahub.model.RecordEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DatahubWrite_java {

    public static void main(String[] args) {

        String projectName = args[0];
        String topicName = args[1];
        String accessId = args[2];
        String accessKey = args[3];
        String vpcEndPoint = args[4];
        long start = 1;
        long sleepTime = 2000;

        // 新建client
        Account account = new AliyunAccount(accessId, accessKey);
        DatahubConfiguration conf = new DatahubConfiguration(account, vpcEndPoint);
        DatahubClient client = new DatahubClient(conf);
        String shardId = "0";
        GetProjectResult getProjectResult = client.getProject(projectName);
        // 构造需要上传的records
        RecordSchema schema = client.getTopic(projectName, topicName).getRecordSchema();

        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        Random random = new Random();
        RecordEntry entry = new RecordEntry(schema);
        while (true) {
            for (int i = 0; i < entry.getFieldCount(); i++) {
                if (i == 0) {
                    entry.setString(i, "name_" + String.format("%05d", start));
                } else if (i == 1) {
                    entry.setString(i, "value_" + String.format("%05d", start));
                }
            }
            recordEntries.add(entry);
            entry = new RecordEntry(schema);
            PutRecordsResult result = client.putRecords(projectName, topicName, recordEntries);
            if (result.getFailedRecordCount() != 0) {
                List<ErrorEntry> errors = result.getFailedRecordError();
                List<RecordEntry> records = result.getFailedRecords();
                // 存在写入失败的记录，建议日志记录错误原因并重试写入
            }
            System.out.println("finish write the " + start + "th record");
            recordEntries.clear();
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            start++;
        }
    }
}
