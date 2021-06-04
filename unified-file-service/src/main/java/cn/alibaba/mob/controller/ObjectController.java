package cn.alibaba.mob.controller;

import cn.alibaba.mob.common.ResultSet;
import cn.alibaba.mob.common.ResultSetCode;
import cn.alibaba.mob.common.TmobUtil;
import cn.alibaba.mob.common.dto.TmobObject;
import cn.alibaba.mob.common.dto.TmobObjectMeta;
import cn.alibaba.mob.service.ObjectService;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping(value="object")
public class ObjectController {


    @Autowired
    private ObjectService objectService;
    
    
    @RequestMapping(value = "/query", method = RequestMethod.POST)
    public ResultSet<List<TmobObject>> query(@RequestParam("bucket") String bucket, String tag) {
        if (StringUtils.isBlank(bucket)) {
            return ResultSet.valueOf(ResultSetCode.SYSTEM_ERROR,
                "bucket can't be empty");
        }
        if (StringUtils.isBlank(tag)) {
            return ResultSet.valueOf(ResultSetCode.SYSTEM_ERROR,
                "tag can't be empty");
        }
        try {
            List<TmobObject> objects = objectService.queryObject(bucket, tag);
            return ResultSet.valueOf(ResultSetCode.SUCCESS, objects);
        } catch (Exception e) {
            return ResultSet.valueOf(ResultSetCode.SYSTEM_ERROR, "query error" + e.getMessage());
        }
    }

    @RequestMapping(value="/upload", method=RequestMethod.POST)
    @ResponseBody
    public ResultSet<JSONObject> upload(@RequestParam("file") MultipartFile file,
        @RequestParam String bucket, @RequestParam("tag") String tag) {
        if (file.isEmpty()) {
            return ResultSet.valueOf(ResultSetCode.SYSTEM_ERROR, "上传失败，请选择文件");
        }
        if (file.getSize() > TmobUtil.OBJ_HBASE_SIZE_MAX) {
            return ResultSet.valueOf(ResultSetCode.SYSTEM_ERROR,
                "get data size is " + file.getSize() + " overhead max size, " + file.getName());
        }
        if (StringUtils.isBlank(tag)) {
            return ResultSet.valueOf(ResultSetCode.SYSTEM_ERROR,
                "tag can't be empty");
        }
        String fileName = file.getOriginalFilename();
        try {
            InputStream is = file.getInputStream();
            ByteArrayOutputStream swapStream = new ByteArrayOutputStream();
            byte[] buff = new byte[1024];
            int rc = 0;
            while ((rc = is.read(buff, 0, 1024)) > 0) {
                swapStream.write(buff, 0, rc);
            }
            String key = String.format("%s_%s_%s", bucket, tag,  UUID.randomUUID().toString());
            byte[] content = swapStream.toByteArray();
            TmobObject object = new TmobObject();
            TmobObjectMeta objectMeta = new TmobObjectMeta();
            objectMeta.setCreated(new Date());
            objectMeta.setName(fileName);
            objectMeta.setSize(file.getSize());
            objectMeta.setTtl(60 * 60 * 1000);
            objectMeta.setTag(tag);

            object.setMeta(objectMeta);
            object.setBucket(bucket);
            object.setData(content);
            object.setKey(key);

            objectService.getObjectMeta(bucket, String.format("%s_%s_%s", bucket, tag, key));
            if (objectService.createObject(object) == null) {
                return ResultSet.valueOf(ResultSetCode.SYSTEM_ERROR, "上传失败!" );
            }
            JSONObject res = new JSONObject();
            res.put("key", key);
            IOUtils.closeQuietly(is);
            return ResultSet.valueOf(ResultSetCode.SUCCESS, res);
        } catch (IOException e) {
            return ResultSet.valueOf(ResultSetCode.SYSTEM_ERROR, e.getMessage());
        }
    }
    
}
