package com.hetao101.SupervisionAnalyse.conf;

import java.io.InputStream;
import java.util.Properties;



//配置管理项
public class ConfigManager {

    private static Properties prop = new Properties();

    static {
        try {
            // 通过调用类加载器（ClassLoader）的getResourceAsStream方法获取指定文件的输入流
            InputStream in = ConfigManager.class
                    .getClassLoader().getResourceAsStream("config.properties");

            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定key对应的value
     * @param key
     * @return value
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    /**
     * 获取整数类型的配置项
     * @param key
     * @return value
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔类型的配置项
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取Long类型的配置项
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }
}
