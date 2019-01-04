package com.aliyun.phoenix.service;
/**
 * This file created by mengqingyi on 2019/1/4.
 */

import com.aliyun.phoenix.entity.Page;
import com.aliyun.phoenix.entity.UserLocation;
import com.aliyun.phoenix.utils.HbaseStringFilter;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;

/**
 * @author mengqingyi
 * @classDescription phoenix使用spring-JDBCTemplate来处理一系列操作,虽然官方不推荐此种用法,但是不可否认连接池搭配spring的jdbcTemplate进一步操作JDBC便捷性大大提升
 * (注:此代码经过线上生产一年多的考验)
 * @create 2019-01-04 11:04
 **/
@Service
public class PhoenixByJDBCTemplateService {
    /**
     * 连接池 BasicDataSource+ spring jdbcTemplete
     */
    @Autowired
    @Qualifier("phoenixJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    /**
     * 记录日志
     */
    private static final Logger logger = LoggerFactory.getLogger("TestPhoenixByJDBCTemplate.class");

    private static final String QUERY_SQL = " select userId,address,lng,lat  from userLocation ";


    /**
     * 默认查询方法
     *
     * @return 返回list集合
     */
    public List<UserLocation> queryDefault() {
        return getUserLocations();
    }


    /**
     * 自定义sql以及查询条件
     */
    public List<UserLocation> query(String whereSql) {
        if (StringUtils.isBlank(whereSql)) {
            return null;
        }
        //自定义sql
        String sql = "select userId,address  from userLocation " + whereSql + " limit 800 ";
        return getUserLocations(sql);
    }

    /**
     * 保存用户地理位置信息
     */
    public void save(UserLocation userLocation) {
        if (judgeKeyParams(userLocation)) { return; }
        //在phoenix中 insert和update二合一,upsert。
        String sql = "upsert into  userLocation values (?,?,?,?,?,?)";
        Object[] args = new Object[6];
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        //(类似于加盐操作)这里简单使用16位 UUID 打散,进而避免热点
        args[0] = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 16);
        args[1] = userLocation.getUserId();
        args[2] = HbaseStringFilter.stringFilter(userLocation.getAddress());
        args[3] = userLocation.getLng();
        args[4] = userLocation.getLat();
        args[5] = simpleDateFormat.format(userLocation.getCreateTime());

        long startTime = System.currentTimeMillis();
        //执行保存操作
        jdbcTemplate.update(sql, args);
        long endTime = System.currentTimeMillis();
        logger.info("用户{}本次执行耗时(ms):{}", userLocation.getUserId(), (endTime - startTime));
    }

    /**
     * 分页查询
     *
     * @param userLocation 实体类
     * @param page         分页参数
     */
    public Page<UserLocation> query(UserLocation userLocation, Page<UserLocation> page) {
        Long userId = userLocation.getUserId();
        if (page == null || userId == null) {
            logger.error("save方法系统关键参数为空,无效保存,不予操作");
            return page;
        }
        //查询总记录数
        Long count = count(userLocation);
        StringBuilder builder = new StringBuilder();
        builder.append(" WHERE USERID = ").append(userId);
        if (page.getOrderBy() != null) {
            builder.append(" ORDER BY ").append(page.getOrderBy());
            if (page.getOrderDir() != null) {
                builder.append(" ").append(page.getOrderDir());
            }
        }
        builder.append(" LIMIT ").append(page.getPageSize()).append(" OFFSET ").append(page.getPageNo());
        String whereSql = builder.toString();
        long startTime = System.currentTimeMillis();
        //查询结果集
        List<UserLocation> query = query(whereSql);
        page.setTotalCount(count);
        page.setResult(query);
        long endTime = System.currentTimeMillis();
        logger.info("本次执行耗时(ms):{}", (endTime - startTime));
        return page;
    }


    public Long count(UserLocation userLocation) {
        if (judgeKeyParams(userLocation)) { return null; }
        String pageSql = " select count(1) from userLocation where userId = " + userLocation.getUserId();
        return jdbcTemplate.queryForLong(pageSql);
    }

    private boolean judgeKeyParams(UserLocation userLocation) {
        if (Objects.isNull(userLocation) || Objects.isNull(userLocation.getUserId())) {
            logger.error("save方法系统关键参数为空,无效保存,不予操作");
            return true;
        }
        return false;
    }

    /**
     * 获取用户地理位置信息 基础方法
     */
    private List<UserLocation> getUserLocations(String sql) {
        long startTime = System.currentTimeMillis();
        List<UserLocation> userLocationList = jdbcTemplate.query(sql, new BeanPropertyRowMapper(UserLocation.class));
        long endTime = System.currentTimeMillis();
        logger.info("本次执行耗时(ms):{}", (endTime - startTime));
        return userLocationList;
    }

    /**
     * 获取默认用户地理位置信息 基础方法
     */
    private List<UserLocation> getUserLocations() {
        return getUserLocations(QUERY_SQL);
    }
}
