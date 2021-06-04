package cn.alibaba.mob.service;

import cn.alibaba.mob.common.TimeUtil;
import cn.alibaba.mob.common.TmobUtil;
import cn.alibaba.mob.common.dto.TmobObject;
import cn.alibaba.mob.common.dto.TmobObjectMeta;
import cn.alibaba.mob.common.dto.TmobType;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

@Service
public class HbaseObjectServiceImpl implements ObjectService {

    private static Logger logger = Logger.getLogger("mob");

    @Autowired
    private HbaseService hbaseService;

    @Autowired
    private HdfsService hdfsService;
    
    @Value("${mob.file.threshold}")
    private Long mobFileThreshold;

    @Override
    public TmobObject getObject(String bucket, String key) {
        TmobObject obj = new TmobObject();
        TmobObjectMeta objMeta = new TmobObjectMeta();
        HTable hTable = null;
        String table = hbaseService.decorateTable(bucket);
        try {
            hTable = (HTable) hbaseService.getConn().getTable(TableName.valueOf(table));
            Result rs = hTable.get(new Get(Bytes.toBytes(key)));

            for (Cell cell : rs.rawCells()) {
                String cf = new String(CellUtil.cloneFamily(cell));
                if (!cf.equals(TmobUtil.COLUMN_FAMILY_DEFAULT)) {
                    continue;
                }

                String col = new String(CellUtil.cloneQualifier(cell));
                byte[] colVal = CellUtil.cloneValue(cell);

                if (col.equals(TmobUtil.OBJ_CONTENT)) {
                    obj.setData(colVal);
                } else if (col.equals(TmobUtil.OBJ_NAMESPACE)) {
                    obj.setBucket(new String(colVal));
                } else if (col.equals(TmobUtil.OBJ_NAME)) {
                    objMeta.setName(new String(colVal));
                } else if (col.equals(TmobUtil.OBJ_SIZE)) {
                    objMeta.setSize(Integer.parseInt(new String(colVal)));
                } else if (col.equals(TmobUtil.OBJ_TTL)) {
                    objMeta.setTtl(Integer.parseInt(new String(colVal)));
                } else if (col.equals(TmobUtil.OBJ_GMTCREATE)) {
                    try {
                        objMeta.setCreated(TimeUtil.parse(new String(colVal)));
                    } catch (ParseException e) {
                    }
                }
            }

            objMeta.setStoreType(TmobType.HBASE.name());
            obj.setMeta(objMeta);
            obj.setKey(key);

        } catch (IOException e) {
            return null;
        }

        return obj;
    }

    @Override
    public TmobObjectMeta getObjectMeta(String bucket, String key) {
        TmobObjectMeta meta = new TmobObjectMeta();
        HTable hTable = null;
        String table = hbaseService.decorateTable(bucket);
        try {
            hTable = (HTable) hbaseService.getConn().getTable(TableName.valueOf(table));
            Result rs = hTable.get(new Get(Bytes.toBytes(key)));

            for (Cell cell : rs.rawCells()) {
                String cf = new String(CellUtil.cloneFamily(cell));
                if (!cf.equals(TmobUtil.COLUMN_FAMILY_DEFAULT)) {
                    continue;
                }
                String col = new String(CellUtil.cloneQualifier(cell));
                String colVal = new String(CellUtil.cloneValue(cell));
                if (col.equals(TmobUtil.OBJ_NAME)) {
                    meta.setName(colVal);
                } else if (col.equals(TmobUtil.OBJ_SIZE)) {
                    meta.setSize(Integer.parseInt(colVal));
                } else if (col.equals(TmobUtil.OBJ_TTL)) {
                    meta.setTtl(Integer.parseInt(colVal));
                } else if (col.equals(TmobUtil.OBJ_GMTCREATE)) {
                    try {
                        meta.setCreated(TimeUtil.parse(colVal));
                    } catch (ParseException e) {
                    }
                } 
            }

            meta.setStoreType(TmobType.HBASE.name());

        } catch (IOException e) {
            return null;
        }

        return meta;
    }

    @Override
    public boolean existObject(String bucket, String key) {
        HTable hTable = null;
        try {
            hTable = (HTable) hbaseService.getConn().getTable(TableName.valueOf(bucket));
            return hTable.exists(new Get(Bytes.toBytes(key)));
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public String createObject(TmobObject object) {
        String bucket = object.getBucket();
        TmobObjectMeta meta = object.getMeta();
        String key = object.getKey();

        try {
            saveMetaInfo(bucket, meta, key);
            saveObject(object, bucket, key, meta.getSize());
        } catch (Exception e) {
            deleteObject(bucket, key);
            logger.error("save object error,", e);
            return null;
        }
        
        return key;
    }
    
    private void saveMetaInfo(String bucket, TmobObjectMeta meta, String key) throws IOException {
        HTable hTable;
        String table = hbaseService.decorateTable(bucket);
        try {
            hTable = (HTable) hbaseService.getConn().getTable(TableName.valueOf(table));
            Put p = new Put(Bytes.toBytes(key));
            p.addColumn(Bytes.toBytes(TmobUtil.COLUMN_FAMILY_DEFAULT),
                Bytes.toBytes(TmobUtil.OBJ_NAME), Bytes.toBytes(meta.getName()));
            p.addColumn(Bytes.toBytes(TmobUtil.COLUMN_FAMILY_DEFAULT),
                Bytes.toBytes(TmobUtil.OBJ_NAMESPACE), Bytes.toBytes(bucket));
            p.addColumn(Bytes.toBytes(TmobUtil.COLUMN_FAMILY_DEFAULT),
                Bytes.toBytes(TmobUtil.OBJ_SIZE), Bytes.toBytes(String.valueOf(meta.getSize())));
            p.addColumn(Bytes.toBytes(TmobUtil.COLUMN_FAMILY_DEFAULT),
                Bytes.toBytes(TmobUtil.OBJ_TTL), Bytes.toBytes(String.valueOf(meta.getTtl())));
            p.addColumn(Bytes.toBytes(TmobUtil.COLUMN_FAMILY_DEFAULT),
                Bytes.toBytes(TmobUtil.OBJ_GMTCREATE), Bytes.toBytes(String.valueOf(meta.getCreated())));

            if (!p.isEmpty()) {
                hTable.put(p);
                logger.info("put hbase " + bucket + " " + key);
            }
        } catch (Exception e) {
            throw e;
        }
    }

    private void saveObject(TmobObject object, String bucket, String key, long size) throws IOException {
        try {
            String table = hbaseService.decorateTable(bucket);
            HTable hTable = (HTable) hbaseService.getConn().getTable(TableName.valueOf(table));
            Put p = new Put(Bytes.toBytes(key));
            if (size < mobFileThreshold) {
                p.addColumn(Bytes.toBytes(TmobUtil.COLUMN_FAMILY_DEFAULT), Bytes.toBytes(TmobUtil.OBJ_CONTENT),
                    object.getData());
                if (!p.isEmpty()) {
                    hTable.put(p);
                    logger.info("put hbase " + bucket + " " + key);
                }
            } else {
                Path path = new Path(String.format("/%s/%s", hdfsService.getRootPath(), bucket));
                FileSystem fileSystem = hdfsService.getFileSystem();
                if (!fileSystem.exists(path)) {
                    fileSystem.mkdirs(path);
                }
                FSDataOutputStream fsDataOutputStream = null;
                try {
                    fsDataOutputStream = fileSystem
                        .create(new Path(String.format("/%s/%s/%s", hdfsService.getRootPath(), bucket, key)), true, 4096);
                    fsDataOutputStream.write(object.getData());
                    fsDataOutputStream.flush();
                } finally {
                    if (fsDataOutputStream != null) {
                        fsDataOutputStream.close();
                    }
                }
                p.addColumn(Bytes.toBytes(TmobUtil.COLUMN_FAMILY_DEFAULT), Bytes.toBytes(TmobUtil.OBJ_CONTENT),
                    Bytes.toBytes(String.format("/%s/%s/%s", hdfsService.getRootPath(), bucket, key)));
                if (!p.isEmpty()) {
                    hTable.put(p);
                }
            }
        } catch (Exception e) {
            throw e;
        }
    }
    
    
    @Override
    public boolean deleteObject(String bucket, String key) {
        HTable hTable = null;
        try {
            hTable = (HTable) hbaseService.getConn().getTable(TableName.valueOf(bucket));
            Delete delete = new Delete(Bytes.toBytes(key));
            hTable.delete(delete);
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    @Override
    public List<TmobObject> queryObject(String bucket, String tag) throws IOException {
        List<TmobObject> objects = new ArrayList<>();
        try {
            String table = hbaseService.decorateTable(bucket);
            HTable hTable = (HTable) hbaseService.getConn().getTable(TableName.valueOf(table));
            String prefixString = String.format("%s_%s",bucket,tag);
            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(prefixString));
            scan.setCaching(10);
            ResultScanner rs = hTable.getScanner(scan);
            for (Result r : rs) {
                TmobObjectMeta objectMeta = getObjectMeta(r);
                TmobObject tmobObject = new TmobObject();
                tmobObject.setMeta(objectMeta);
                objectMeta.setStoreType(TmobType.HBASE.name());
                tmobObject.setMeta(objectMeta);
                tmobObject.setKey(Bytes.toHex(r.getRow()));
                if (objectMeta.getSize() < mobFileThreshold) {
                    tmobObject.setData(r.getValue(Bytes.toBytes(TmobUtil.COLUMN_FAMILY_DEFAULT),
                        Bytes.toBytes(TmobUtil.OBJ_CONTENT)));
                } else {
                    FileSystem fileSystem = hdfsService.getFileSystem();
                    String filePath = Bytes.toString(r.getValue(Bytes.toBytes(TmobUtil.COLUMN_FAMILY_DEFAULT),
                        Bytes.toBytes(TmobUtil.OBJ_CONTENT)));
                    if (filePath == null || !fileSystem.exists(new Path(filePath))) {
                        continue;
                    }
                    objectMeta.setStoreType(TmobType.HDFS.name());
                    FSDataInputStream inputStream = fileSystem.open(new Path(filePath));
                    byte[] content = new byte[inputStream.available()];
                    inputStream.readFully(content);
                    tmobObject.setData(content);
                }
                objects.add(tmobObject);
            }
        } catch (Exception e) {
            logger.error("query error,{}", e);
            throw e;
        }
        return objects;
    }

    private TmobObjectMeta getObjectMeta(Result r) {
        TmobObjectMeta objMeta = new TmobObjectMeta();
        for (Cell cell : r.rawCells()) {
            String cf = new String(CellUtil.cloneFamily(cell));
            if (!cf.equals(TmobUtil.COLUMN_FAMILY_DEFAULT)) {
                continue;
            }
            String col = new String(CellUtil.cloneQualifier(cell));
            String colVal = new String(CellUtil.cloneValue(cell));
            if (col.equals(TmobUtil.OBJ_NAME)) {
                objMeta.setName(colVal);
            } else if (col.equals(TmobUtil.OBJ_SIZE)) {
                objMeta.setSize(Integer.parseInt(colVal));
            } else if (col.equals(TmobUtil.OBJ_TTL)) {
                objMeta.setTtl(Integer.parseInt(colVal));
            } else if (col.equals(TmobUtil.OBJ_GMTCREATE)) {
                try {
                    objMeta.setCreated(TimeUtil.parse(colVal));
                } catch (ParseException e) {
                }
            }
        }
        objMeta.setStoreType(TmobType.HBASE.name());
        return objMeta;
    }
}
