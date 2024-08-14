package com.at.rt.data.warehouse.utils;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.*;

/**
 * @author wenzhilong
 */
public class YamlUtil {

    private static final Logger logger = LoggerFactory.getLogger(YamlUtil.class);

    public static final Map<Object, Object> properties = init();

    static Map<Object, Object> init() {

        String sysConf = System.getProperty("system.conf.file");
        if (StringUtils.isBlank(sysConf)) {
            sysConf = System.getenv("system.conf.file");
        }

        if (StringUtils.isBlank(sysConf)) {
            sysConf = "/production.yaml";
        }

        HashMap<Object, Object> configMap = new HashMap<>(64);

        try {

            Yaml yaml = new Yaml();
            InputStream is = YamlUtil.class.getResourceAsStream(sysConf);

            configMap = yaml.loadAs(is, HashMap.class);
        } catch (Exception e) {
            logger.error("readConfigFile error fileName = {} error message = {}", sysConf, e.getMessage());
        }

        logger.info("parse yaml file, file = {}, content: {}", sysConf, configMap);

        return configMap;
    }

    public static Map<String, String> parseYaml() {

        if (properties == null || properties.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> configMap = new HashMap<>(128);

        try {
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {

                String key = String.valueOf(entry.getKey());
                Object value = entry.getValue();

                if (value instanceof Map) {
                    parseYamlToStr((Map<Object, Object>) value, key, configMap);
                } else {
                    configMap.put(key, String.valueOf(value));
                }
            }
        } catch (Exception e) {
            logger.error("parse yaml to string map error, configMap = {}", configMap, e);
        }

        logger.info("parse yaml content: {}", configMap);

        return configMap;
    }

    private static void parseYamlToStr(Map<Object, Object> configMapObj, String key, Map<String, String> configMap) {
        for (Map.Entry<Object, Object> entry : configMapObj.entrySet()) {

            String kk = key + "." + entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                configMap.put(kk, String.valueOf(entry.getKey()));
                parseYamlToStr((Map<Object, Object>) value, kk, configMap);
            } else if (value instanceof List) {
                for (Object v : (List) value) {
                    if (v instanceof Map) {
                        parseYamlToStr((Map<Object, Object>) v, kk, configMap);
                    }
                }
            } else {
                configMap.put(kk, String.valueOf(value));
            }
        }
    }

    public static Object getValueByKey(String key) {

        String separator = ".";
        String[] separatorKeys = null;
        if (key.contains(separator)) {
            // 取下面配置项的情况, user.path.keys 这种
            separatorKeys = key.split("\\.");
        } else {
            // 直接取一个配置项的情况, user
            Object res = properties.get(key);
            return res;
        }
        // 下面肯定是取多个的情况
        String finalValue = null;
        Object tempObject = properties;
        for (int i = 0; i < separatorKeys.length; i++) {
            //如果是user[0].path这种情况,则按list处理
            String innerKey = separatorKeys[i];
            String index = null;
            if (innerKey.contains("[")) {
                // 如果是user[0]的形式,则index = 0 , innerKey=user
                // 如果是user[name]的形式,则index = name , innerKey=user
                index = StringUtils.substringsBetween(innerKey, "[", "]")[0];

                innerKey = innerKey.substring(0, innerKey.indexOf("["));
            }
            Map<String, Object> mapTempObj = (Map) tempObject;
            Object object = mapTempObj.get(innerKey);
            // 如果没有对应的配置项,则返回设置的默认值
            if (object == null) {
                return null;
            }
            Object targetObj = object;
            if (index != null) {
                // 如果是取的数组中的值,在这里取值
                if (StringUtils.isNumeric(index)) {
                    targetObj = ((ArrayList) object).get(Integer.valueOf(index));
                } else {

                    final ArrayList<Map<String, Object>> mapArrayList = (ArrayList<Map<String, Object>>) object;

                    for (Map<String, Object> objectMap : mapArrayList) {
                        if (objectMap.keySet().contains(index)) {
                            targetObj = objectMap.get(index);
                            break;
                        }
                    }

                }
            }
            // 一次获取结束,继续获取后面的
            tempObject = targetObj;
            if (i == separatorKeys.length - 1) {
                //循环结束
                return targetObj;
            }

        }
        return null;
    }

    public static <T> T getOrDefault(String key, T defaultVal) {
        return (T) ObjectUtils.defaultIfNull(getValueByKey(key), defaultVal);
    }

    public static String getString(String key) {
        return String.valueOf(getValueByKey(key));
    }

    public static Integer getInt(String key) {
        return ((Number) getValueByKey(key)).intValue();
    }

    public static Long getLong(String key) {
        return ((Number) getValueByKey(key)).longValue();
    }

    public static Boolean getBoolean(String key) {
        return (Boolean) getValueByKey(key);
    }
}
