package com.xiaoxiaomo.metrics.utils;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by TangXD on 2017/8/2.
 */
public class ConfigLoader {

    private static Properties properties = null;

    static {
        ConfigLoader.properties = new Properties();
        try{
            ConfigLoader.properties.load(ConfigLoader.class.getResourceAsStream("/opentsdb.properties"));
        }catch (IOException e){
            e.printStackTrace();
            throw new RuntimeException("加载配置文件出错");
        }
    }


    public static String getProperty(String key){
        return ConfigLoader.properties.getProperty(key);
    }

}
