package cn.alibaba.mob.service;

import cn.alibaba.mob.common.dto.TmobBucket;

import java.io.IOException;
import java.util.List;

public interface BucketService {

    boolean existBucket(String bucketName) throws IOException;

    boolean createBucket(TmobBucket bucket) throws IOException;

    boolean deleteBucket(String bucketName) throws IOException;

    TmobBucket getBucket(String bucketName) throws IOException;
    
    List<TmobBucket> list() throws IOException;
    
}
