package com.aliyun.phoenix.mybatis.bean;

/**
 * @author 长轻
 * @date 2019/8/15 5:07 PM
 */
public class UsPopulationDO {

    private String state;

    private String city;

    private Long population;

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Long getPopulation() {
        return population;
    }

    public void setPopulation(Long population) {
        this.population = population;
    }
}
