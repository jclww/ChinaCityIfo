package com.lww.city;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

public class MysqlConnect {
    private static final Logger log = LoggerFactory.getLogger(MysqlConnect.class);

    public static void insertDBMap(List<Map<String,Object>> cityList) throws SQLException, ParseException {
        Connection connection = DriverManager.getConnection(Config.URL, Config.USER, Config.PASSWORD);
        PreparedStatement psql = connection.prepareStatement(
                "insert into " + Config.TABLE +
                        "(code,name,tree_path,parent_code,level) "
                        + "values(?,?,?,?,?)");
        for (Map map : cityList) {
            psql.setString(1, (String) map.get("code"));
            psql.setString(2, (String) map.get("name"));
            psql.setString(3, (String) map.get("path"));
            psql.setString(4, (String) map.get("parentCode"));
            psql.setInt(5, (Integer) map.get("level"));
            log.info("insert:{}", JSON.toJSONString(map));
            psql.addBatch();
        }
        psql.executeBatch();
        connection.close();
    }
}
