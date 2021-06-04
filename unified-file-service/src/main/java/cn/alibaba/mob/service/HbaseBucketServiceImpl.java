package cn.alibaba.mob.service;

import cn.alibaba.mob.common.TmobUtil;
import cn.alibaba.mob.common.dto.TmobBucket;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@Service
public class HbaseBucketServiceImpl implements BucketService {

    @Autowired
    private HbaseService hbaseService;

    @Override
    public boolean existBucket(String bucketName) throws IOException {
        Admin admin = hbaseService.getConn().getAdmin();
        return admin.tableExists(TableName.valueOf(bucketName));
    }

    @Override
    public boolean createBucket(TmobBucket bucket) throws IOException {
        Connection connection = hbaseService.getConn();
        Admin admin = connection.getAdmin();
        if (!existBucket(bucket.getName())) {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(bucket.getName()));
            HColumnDescriptor hcd = new HColumnDescriptor(TmobUtil.COLUMN_FAMILY_DEFAULT);
            tableDesc.addFamily(hcd);

            admin.createTable(tableDesc);

            Put put = new Put(Bytes.toBytes(String.format("bucket_info")));
            put.addColumn(Bytes.toBytes(TmobUtil.COLUMN_FAMILY_DEFAULT), Bytes.toBytes(TmobUtil.BUCKET_INFO_COLUMN),
                Bytes.toBytes(bucket.toString()));
            Table table = connection.getTable(TableName.valueOf(bucket.getName()));
            table.put(put);
            
        }
        return true;
    }

    @Override
    public boolean deleteBucket(String bucketName) throws IOException {
        Admin admin = hbaseService.getConn().getAdmin();
        if (admin.tableExists(TableName.valueOf(bucketName))) {
            admin.disableTable(TableName.valueOf(bucketName));
            admin.deleteTable(TableName.valueOf(bucketName));
        }
        return true;
    }

    @Override
    public TmobBucket getBucket(String bucketName) throws IOException {
        Connection connection = hbaseService.getConn();
        Table table = connection.getTable(TableName.valueOf(bucketName));
        TmobBucket tmobBucket = getTmobBucket(table);
        return tmobBucket;
    }

    private TmobBucket getTmobBucket(Table table) throws IOException {
        Get get = new Get(Bytes.toBytes(String.format("bucket_info")));
        Result result = table.get(get);
        if (result != null && !result.isEmpty()) {
            TmobBucket tmobBucket = new TmobBucket();
            Cell cell = result.getColumnLatestCell(Bytes.toBytes(TmobUtil.COLUMN_FAMILY_DEFAULT),
                Bytes.toBytes(TmobUtil.BUCKET_INFO_COLUMN));
            if (cell != null) {
                JSONObject object = JSONObject.parseObject(Bytes.toString(CellUtil.cloneValue(cell)));
                tmobBucket.setName(object.getString("name"));
                tmobBucket.setOwner(object.getString("owner"));
                tmobBucket.setRegion(object.getString("region"));
                tmobBucket.setCreated(object.getDate("created"));
            } else {
                return null;
            }
            return tmobBucket;
        }
        return null;
    }

    @Override public List<TmobBucket> list() throws IOException {
        Connection connection = hbaseService.getConn();
        Admin admin = connection.getAdmin();
        List<TmobBucket> tmobBuckets = new LinkedList<>();
        for (TableDescriptor tableDescriptor : admin.listTableDescriptors()) {
            if (tableDescriptor.getTableName().getNameAsString().equals("IntegrationTestIngestWithMOB")) {
                continue;
            }
            Table table = connection.getTable(TableName.valueOf(tableDescriptor.getTableName().getName()));
            TmobBucket tmobBucket = getTmobBucket(table);
            if (tmobBucket != null) {
                tmobBuckets.add(tmobBucket);
            }
        }
        return tmobBuckets;
    }
}
