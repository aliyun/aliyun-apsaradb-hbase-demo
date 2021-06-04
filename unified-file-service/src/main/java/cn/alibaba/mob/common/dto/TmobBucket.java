package cn.alibaba.mob.common.dto;


import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.util.Date;

public class TmobBucket implements Serializable {

    private String name;

    private String region;

    private String owner;

    private Date created;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    @Override public String toString() {
        JSONObject object = new JSONObject();
        object.put("name", name);
        object.put("region", region);
        object.put("owner", owner);
        object.put("created", created);
        return object.toJSONString();
    }
}
