package com.aliyun.phoenix.service;
/**
 * This file created by mengqingyi on 2019/1/4.
 */

import com.aliyun.phoenix.entity.UserLocation;
import com.aliyun.phoenix.utils.HbaseStringFilter;
import com.aliyun.phoenix.utils.pool.PhoenixConnectionByJDBC;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

/**
 * @author mengqingyi
 * @classDescription 使用普通的jdbc测试连接处理phoenix
 * @create 2019-01-04 14:17
 **/
public class PhoenixByJDBCService {

    private static final Logger logger = LoggerFactory.getLogger(PhoenixByJDBCService.class);

    /**
     * 保存
     */
    private final static String SALT_PHONECONTACTS_UPSERT_SQL =
            "UPSERT INTO SALT_PHONECONTACTS(userId,address,lng,lat,create_time) VALUES (?,?,?,?,?)";

    public void save(List<UserLocation> userLocationList) {
        if (CollectionUtils.isEmpty(userLocationList)) {
            logger.error("save方法系统关键参数为空,无效保存,不予操作");
            return;
        }
        Connection connection = null;
        PreparedStatement statement = null;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            connection = PhoenixConnectionByJDBC.getConnection();
            connection.setAutoCommit(false);
        } catch (Exception e) {
            logger.error("[hbase salt_phoneContacts]基础查询获取连接失败!!!{},\r\n堆栈信息{}", e.getMessage(), e);
            e.printStackTrace();
        }
        try {
            long start = System.currentTimeMillis();
            statement = connection.prepareStatement(SALT_PHONECONTACTS_UPSERT_SQL);

            for (UserLocation userLocation : userLocationList) {
                //若为加盐表 需要注意值判空,存在空值跳过
                statement.setLong(1, userLocation.getUserId());
                statement.setString(2, HbaseStringFilter.stringFilter(userLocation.getAddress()));
                statement.setDouble(3, userLocation.getLng());
                statement.setDouble(4, userLocation.getLat());
                statement.setDate(5, (Date) userLocation.getCreateTime());
                statement.addBatch();
            }
            int[] execute = statement.executeBatch();
            connection.commit();
            long end = System.currentTimeMillis();
            logger.info("save保存方法影响行数{},耗时{}ms", execute, (end - start));
        } catch (Exception e) {
            logger.error("save保存方法发生异常{},堆栈信息{}", e.getMessage(), e);
        } finally {
            Boolean close = PhoenixConnectionByJDBC.close(connection, statement);
            if (!close) {
                logger.error("save资源关闭异常!!!");
            }
        }

    }

}
