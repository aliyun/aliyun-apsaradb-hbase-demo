package cn.alibaba.mob.service;

import cn.alibaba.mob.common.dto.TmobObject;
import cn.alibaba.mob.common.dto.TmobObjectMeta;

import java.io.IOException;
import java.util.List;

public interface ObjectService {

    TmobObject getObject(String bucket, String key);

    TmobObjectMeta getObjectMeta(String bucket, String key);

    boolean existObject(String bucket, String key);

    String createObject(TmobObject object);

    boolean deleteObject(String bucket, String key);
    
    List<TmobObject> queryObject(String bucket, String tag) throws IOException;
    
}
