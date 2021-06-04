package cn.alibaba.mob.controller;

import cn.alibaba.mob.common.ResultSet;
import cn.alibaba.mob.common.ResultSetCode;
import cn.alibaba.mob.common.dto.TmobBucket;
import cn.alibaba.mob.common.dto.TmobRegion;
import cn.alibaba.mob.service.BucketService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Date;
import java.util.List;

@RestController
@RequestMapping(value="bucket")
public class BucketController {

    @Autowired
    private BucketService bucketService;
    
    
    @RequestMapping(value="/create", method=RequestMethod.POST)
    public ResultSet<String> create(HttpServletRequest request, @RequestParam String bucket) {
        if (StringUtils.isEmpty(bucket)) {
          return ResultSet.valueOf(ResultSetCode.PARAM_ERROR, "bucketName不能为空");
        }

        try {
            TmobBucket tmobBucket = new TmobBucket();
            tmobBucket.setName(bucket);
            tmobBucket.setCreated(new Date());
            tmobBucket.setOwner("Dfs");
            tmobBucket.setRegion(TmobRegion.HZ.name());
            bucketService.createBucket(tmobBucket);
        } catch (IOException e) {
            return ResultSet.valueOf(ResultSetCode.SYSTEM_ERROR, e.getMessage());
        }
        return ResultSet.valueOf(ResultSetCode.SUCCESS);
    }

    @RequestMapping(value = "/list", method = RequestMethod.GET)
    public ResultSet<List<TmobBucket>> list(
        HttpServletRequest request) {
      try {
        return ResultSet.valueOf(ResultSetCode.SUCCESS, bucketService.list());
      } catch (IOException e) {
        return ResultSet.valueOf(ResultSetCode.SYSTEM_ERROR, e.getMessage());
      }
    }

    @RequestMapping(value = "/getBucketInfo", method = RequestMethod.GET)
    public ResultSet<TmobBucket> getBucketInfo(
        HttpServletRequest request, @RequestParam String bucket) {
      try {
        return ResultSet.valueOf(ResultSetCode.SUCCESS, bucketService.getBucket(bucket));
      } catch (IOException e) {
        return ResultSet.valueOf(ResultSetCode.SYSTEM_ERROR, e.getMessage());
      }
    }

    @RequestMapping(value="/delete", method=RequestMethod.POST)
    public ResultSet<String> delete(HttpServletRequest request, @RequestParam String bucket) {
        if (StringUtils.isEmpty(bucket)) {
            return ResultSet.valueOf(ResultSetCode.PARAM_ERROR, "bucketName不能为空");
        }
        try {
            bucketService.deleteBucket(bucket);
        } catch (IOException e) {
            return ResultSet.valueOf(ResultSetCode.SYSTEM_ERROR, e.getMessage());
        }
        return ResultSet.valueOf(ResultSetCode.SUCCESS);
    }

}
