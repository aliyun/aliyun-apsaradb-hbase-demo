package com.aliyun.phoenix.mybatis.mapper;

import java.util.List;

import com.aliyun.phoenix.mybatis.bean.UsPopulationDO;

/**
 * @author 长轻
 * @date 2019/8/14 5:29 PM
 */
public interface UsPopulationMapper {

    List<UsPopulationDO> selectByExample(UsPopulationDO usPopulationDO);

    int upsertByExample(UsPopulationDO usPopulationDO);

}
