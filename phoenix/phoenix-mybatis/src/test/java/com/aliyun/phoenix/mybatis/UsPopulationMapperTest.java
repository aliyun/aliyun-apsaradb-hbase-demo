package com.aliyun.phoenix.mybatis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import com.aliyun.phoenix.mybatis.bean.UsPopulationDO;
import com.aliyun.phoenix.mybatis.mapper.UsPopulationMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.phoenix.queryserver.client.Driver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class UsPopulationMapperTest {

    private final static String TABLE_NAME = "us_population";

    @Value("${spring.datasource.phoenix.server.url}")
    private String phoenixServerUrl;

    @Autowired
    private UsPopulationMapper usPopulationMapper;

    @Before
    public void createTestData() throws SQLException {
        this.createTestTable();
        this.insertTestData();
    }

    private void createTestTable() throws SQLException {
        try (Connection conn = getConnection(phoenixServerUrl)) {
            conn.createStatement().execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS us_population (\n"
                + "   state CHAR(2) NOT NULL,\n"
                + "   city VARCHAR NOT NULL,\n"
                + "   population BIGINT\n"
                + "   CONSTRAINT my_pk PRIMARY KEY (state, city))");
        }
    }

    private void insertTestData() throws SQLException {
        try (Connection conn = getConnection(phoenixServerUrl);) {
            conn.createStatement().executeUpdate("UPSERT INTO us_population VALUES('NY','New York',8143197)");
            conn.createStatement().executeUpdate("UPSERT INTO us_population VALUES('CA','Los Angeles',3844829)");
            conn.createStatement().executeUpdate("UPSERT INTO us_population VALUES('IL','Chicago',2842518)");
            conn.createStatement().executeUpdate("UPSERT INTO us_population VALUES('TX','Houston',2016582)");
            conn.createStatement().executeUpdate("UPSERT INTO us_population VALUES('PA','Philadelphia',1463281)");
            conn.createStatement().executeUpdate("UPSERT INTO us_population VALUES('AZ','Phoenix',1461575)");
            conn.createStatement().executeUpdate("UPSERT INTO us_population VALUES('TX','San Antonio',1256509)");
            conn.createStatement().executeUpdate("UPSERT INTO us_population VALUES('CA','San Diego',1255540)");
            conn.createStatement().executeUpdate("UPSERT INTO us_population VALUES('TX','Dallas',1213825)");
        }

    }

    @After
    public void cleanTestData() throws SQLException {
        try (Connection conn = getConnection(phoenixServerUrl)) {
            conn.createStatement().execute("DROP TABLE IF EXISTS " + TABLE_NAME);
        }
    }

    @Test
    public void testSelectByExample() throws Exception {
        UsPopulationDO usPopulationDO = new UsPopulationDO();
        usPopulationDO.setState("NY");
        usPopulationDO.setCity("New York");
        List<UsPopulationDO> nyPopulations = usPopulationMapper.selectByExample(usPopulationDO);

        assert CollectionUtils.isNotEmpty(nyPopulations);
        assert 1 == nyPopulations.size();
        assert nyPopulations.get(0).getPopulation().equals(8143197L);
    }

    @Test
    public void testUpsertByExample() throws Exception {
        this.testUpsertCASanJoseRow(912332L);
        this.testUpsertCASanJoseRow(10000000L);
    }

    private void testUpsertCASanJoseRow(Long population) {
        UsPopulationDO usPopulationDO = new UsPopulationDO();
        usPopulationDO.setState("CA");
        usPopulationDO.setCity("San Jose");
        usPopulationDO.setPopulation(population);

        Integer updateRow = usPopulationMapper.upsertByExample(usPopulationDO);
        assert Integer.valueOf(1).equals(updateRow);

        UsPopulationDO queryParam = new UsPopulationDO();
        queryParam.setState("CA");
        queryParam.setCity("San Jose");

        List<UsPopulationDO> sjPopulations = usPopulationMapper.selectByExample(queryParam);
        assert CollectionUtils.isNotEmpty(sjPopulations);
        assert 1 == sjPopulations.size();
        assert population.equals(sjPopulations.get(0).getPopulation());

    }

    private Connection getConnection(String queryServerAddress) {
        String url = String.format("jdbc:phoenix:thin:url=%s;serialization=PROTOBUF", queryServerAddress);
        try {
            Class.forName(Driver.class.getName());
            return DriverManager.getConnection(url);
        } catch (Exception e) {
            throw new RuntimeException("Load driver [" + Driver.class.getName() + "] failed!", e);
        }
    }

}