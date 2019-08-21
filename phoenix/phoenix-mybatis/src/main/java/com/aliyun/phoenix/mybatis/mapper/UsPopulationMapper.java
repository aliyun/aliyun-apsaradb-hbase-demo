package com.aliyun.phoenix.mybatis.mapper;

import java.util.List;

import com.aliyun.phoenix.mybatis.bean.UsPopulationDO;

public interface UsPopulationMapper {

    /**
     * 根据参数查询符合条件的美国人口列表
     *
     * @param usPopulationDO 美国人口查询对象
     * @return 符合条件的美国人口对象列表
     */
    List<UsPopulationDO> selectByExample(UsPopulationDO usPopulationDO);

    /**
     * 插入一条美国人口信息
     *
     * @param usPopulationDO 美国人口数据库对象
     * @return 变更的行数
     */
    int upsertByExample(UsPopulationDO usPopulationDO);

}
