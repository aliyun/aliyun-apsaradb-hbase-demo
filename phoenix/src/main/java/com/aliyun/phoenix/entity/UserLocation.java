package com.aliyun.phoenix.entity;
/**
 * This file created by mengqingyi on 2019/1/4.
 */

import java.util.Date;

import lombok.Data;

/**
 * @author mengqingyi
 * @classDescription
 * @create 2019-01-04 11:14
 **/
@Data
public class UserLocation {

    private String id;
    /**
     * userId : 用户id
     */
    private Long userId;
    /**
     * address : 地址名称
     */
    private String address;
    /**
     * lng : 经度
     */
    private Double lng;
    /**
     * lat : 纬度
     */
    private Double lat;
    /**
     * createTime :记录时间
     */
    private Date createTime;

}
