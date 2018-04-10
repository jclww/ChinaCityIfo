package com.lww.city;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mysql.jdbc.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * start
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        String cityJson = getFileInfoJson(Config.LIST_PWD);
        // Map
        JSONObject jsonObject = JSON.parseObject(cityJson);
        List<Map.Entry<String, Object>> cityList = jsonObject.entrySet().stream()
                .filter(entry -> getCityLevel(entry.getKey()) != 1)
                .collect(Collectors.toList());
        // 先入库 一级/二级/三级城市
        long startTime = System.currentTimeMillis();
        insertDB(jsonObject.entrySet());
        long insertCityTime = System.currentTimeMillis();
        // 查询街道信息 入库
        getStreetInfo(cityList);
        long endTime = System.currentTimeMillis();
        log.info("城市信息入库 耗时：{} ms", insertCityTime - startTime);
        log.info("城镇信息入库 耗时：{} ms", endTime - insertCityTime);
        log.info("总耗时 耗时：{} ms", endTime - startTime);
    }


    private static void getStreetInfo(List<Map.Entry<String, Object>> entryList) {
        log.info("......getStreetInfo......");
        entryList.forEach(entry -> {
            String streetFilePwd = Config.TOWN_PWD + entry.getKey() + Config.JSON_SUFFIX;
            String streetJson = getFileInfoJson(streetFilePwd);
            if (!StringUtils.isNullOrEmpty(streetJson)) {
                JSONObject streetObject = JSON.parseObject(streetJson);
                insertDB(streetObject, entry);
            }
        });
    }

    private static void insertDB(Set<Map.Entry<String, Object>> entryList) {
        List<Map<String, Object>> mapList = entryList.stream().map(entry -> {
            Map<String, Object> map = new HashMap<>();
            map.put("code", entry.getKey());
            map.put("name", entry.getValue());
            map.put("path", getPath(entry.getKey()));
            map.put("parentCode", getParentCode(entry.getKey()));
            map.put("level", getCityLevel(entry.getKey()));
            return map;
        }).collect(Collectors.toList());
        batchesInsertDB(mapList);
    }

    private static void insertDB(JSONObject streetObject, Map.Entry<String, Object> parent) {
        List<Map<String, Object>> mapList = streetObject.entrySet().stream().map(entry -> {
            Map<String, Object> map = new HashMap<>();
            map.put("code", entry.getKey());
            map.put("name", entry.getValue());
            map.put("path", getPath(parent.getKey()) + "," + entry.getKey());
            map.put("parentCode", parent.getKey());
            map.put("level", getCityLevel(parent.getKey()) + 1);
            return map;
        }).collect(Collectors.toList());
        batchesInsertDB(mapList);
    }

    private static void batchesInsertDB(List<Map<String, Object>> mapList) {
        try {
            for (int i = 0; i < mapList.size(); ) {
                if (i + 100 >= mapList.size()) {
                    MysqlConnect.insertDBMap(mapList.subList(i, mapList.size()));
                } else {
                    MysqlConnect.insertDBMap(mapList.subList(i, i + 100));
                }
                i += 100;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private static String getPath(String cityCode) {
        int num = Integer.parseInt(cityCode);
        if (num % 10000 == 0) {
            return cityCode;
        }
        if (num % 100 == 0 || num % 10000 >= 9000) {
            return num / 10000 + "0000," + num;
        }
        return num / 10000 + "0000," + num / 100 + "00," + num;
    }

    private static String getParentCode(String cityCode) {
        int num = Integer.parseInt(cityCode);
        if (num % 10000 == 0) {
            return "0";
        }
        if (num % 100 == 0 || num % 10000 >= 9000) {
            return num / 10000 + "0000";
        }
        return num / 100 + "00";
    }

    private static int getCityLevel(String key) {
        int num = Integer.parseInt(key);
        if (num % 10000 == 0) {
            return 1;
        }
        // 存在一些大于等于9000的城市也是二级城市
        // 429001——湖北省随州市 429002——湖北省老河口市
        if (num % 100 == 0 || num % 10000 >= 9000) {
            return 2;
        }
        return 3;
    }

    private static String getFileInfoJson(String filePwd) {
        File file = new File(filePwd);
        try {
            System.out.println(file.getCanonicalPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
        Long fileLength = file.length();
        byte[] fileContent = new byte[fileLength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(fileContent);
            in.close();
        } catch (FileNotFoundException e) {
            log.error(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
        String cityJson = null;
        try {
            cityJson = new String(fileContent, Config.CHARSET);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return cityJson;
    }
}
