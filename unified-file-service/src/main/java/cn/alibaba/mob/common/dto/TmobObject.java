package cn.alibaba.mob.common.dto;


import java.io.Serializable;

public class TmobObject implements Serializable {

    private String key;

    private String bucket;

    private TmobObjectMeta meta;
    
    

    private byte[] data;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public TmobObjectMeta getMeta() {
        return meta;
    }

    public void setMeta(TmobObjectMeta meta) {
        this.meta = meta;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}