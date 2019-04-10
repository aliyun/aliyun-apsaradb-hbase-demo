package com.aliyun.spark;

public class Message {
    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String key;
    public String value;

    Message(String key, String value) {
        this.key = key;
        this.value = value;
    }

}
