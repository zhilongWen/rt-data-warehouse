package com.at.rt.data.warehouse.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * @author wenzhilong
 */
public class JSONWrapper {

    private static final Logger logger = LoggerFactory.getLogger(JSONWrapper.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * 时间日期格式
     */
    private static final String STANDARD_FORMAT = "yyyy-MM-dd HH:mm:ss";

    // 以静态代码块初始化
    static {
        //对象的所有字段全部列入序列化
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        //取消默认转换timestamps形式
        OBJECT_MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        //忽略空Bean转json的错误
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        //所有的日期格式都统一为以下的格式，即yyyy-MM-dd HH:mm:ss
        OBJECT_MAPPER.setDateFormat(new SimpleDateFormat(STANDARD_FORMAT));
        //忽略 在json字符串中存在，但在java对象中不存在对应属性的情况。防止错误
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }


    /**
     * ===========================以下是从JSON中获取对象====================================
     */

    public static <T> T parseObject(String data, Class<T> object) {
        T t = null;
        try {
            t = OBJECT_MAPPER.readValue(data, object);
        } catch (JsonProcessingException e) {
            logger.error("JsonString 转为自定义对象失败: data = {}, err = {}", data, e.getMessage());
        }
        return t;
    }

    public static <T> T parseObject(File file, Class<T> object) {
        T t = null;
        try {
            t = OBJECT_MAPPER.readValue(file, object);
        } catch (IOException e) {
            logger.error("从文件中读取 json 字符串转为自定义对象失败：{}", e.getMessage());
        }
        return t;
    }

    /**
     * 将json数组字符串转为指定对象List列表或者Map集合
     *
     * @param jsonArray
     * @param reference
     * @param <T>
     * @return
     */
    public static <T> T parseJSONArray(String jsonArray, TypeReference<T> reference) {
        T t = null;
        try {
            t = OBJECT_MAPPER.readValue(jsonArray, reference);
        } catch (JsonProcessingException e) {
            logger.error("JSONArray转为List列表或者Map集合失败：{}", e.getMessage());
        }
        return t;
    }


    /**
     * =================================以下是将对象转为JSON=====================================
     */

    public static String toJSONString(Object object) {
        String jsonString = null;
        try {
            jsonString = OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            logger.error("Object转JSONString失败：{}", e.getMessage());
        }
        return jsonString;
    }

    public static String toJSONStringSnake(Object object) {
        String jsonString = null;
        try {
            OBJECT_MAPPER.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
            jsonString = OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            logger.error("Object转JSONString失败：{}", e.getMessage());
        }
        return jsonString;
    }

    public static byte[] toByteArray(Object object) {
        byte[] bytes = null;
        try {
            bytes = OBJECT_MAPPER.writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            logger.error("Object转ByteArray失败：{}", e.getMessage());
        }
        return bytes;
    }

    public static void objectToFile(File file, Object object) {
        try {
            OBJECT_MAPPER.writeValue(file, object);
        } catch (JsonProcessingException e) {
            logger.error("Object写入文件失败：{}", e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * =============================以下是与JsonNode相关的=======================================
     */

    /**
     * JsonNode和JSONObject一样，都是JSON树形模型，只不过在jackson中，存在的是JsonNode
     *
     * @param jsonString
     * @return
     */
    public static JsonNode parseJSONObject(String jsonString) {
        JsonNode jsonNode = null;
        try {
            jsonNode = OBJECT_MAPPER.readTree(jsonString);
        } catch (JsonProcessingException e) {
            logger.error("JSONString 转为 JsonNode 失败：{}", e.getMessage());
        }
        return jsonNode;
    }

    public static JsonNode parseJSONObject(Object object) {
        return OBJECT_MAPPER.valueToTree(object);
    }

    public static String toJSONString(JsonNode jsonNode) {
        String jsonString = null;
        try {
            jsonString = OBJECT_MAPPER.writeValueAsString(jsonNode);
        } catch (JsonProcessingException e) {
            logger.error("JsonNode转JSONString失败：{}", e.getMessage());
        }
        return jsonString;
    }

    /**
     * JsonNode 是一个抽象类，不能实例化，创建JSON树形模型，得用JsonNode的子类ObjectNode，用法和JSONObject大同小异
     *
     * @return
     */
    public static ObjectNode newJSONObject() {
        return OBJECT_MAPPER.createObjectNode();
    }

    /**
     * 创建JSON数组对象，就像JSONArray一样用
     *
     * @return
     */
    public static ArrayNode newJSONArray() {
        return OBJECT_MAPPER.createArrayNode();
    }


    /**
     * ===========以下是从JsonNode对象中获取key值的方法，个人觉得有点多余，直接用JsonNode自带的取值方法会好点，出于纠结症，还是补充进来了
     */

    public static String getString(JsonNode jsonObject, String key) {

        if (jsonObject == null) {
            return null;
        }

        if (jsonObject.hasNonNull(key)) {
            return jsonObject.get(key).asText();
        }
        return null;
    }

    public static Integer getInteger(JsonNode jsonObject, String key) {

        if (jsonObject == null) {
            return null;
        }

        if (jsonObject.hasNonNull(key)) {
            return jsonObject.get(key).asInt();
        }
        return null;
    }

    public static Long getLong(JsonNode jsonObject, String key) {

        if (jsonObject == null) {
            return null;
        }

        if (jsonObject.hasNonNull(key)) {
            return jsonObject.get(key).asLong();
        }
        return null;
    }

    public static Boolean getBoolean(JsonNode jsonObject, String key) {

        if (jsonObject == null) {
            return null;
        }

        if (jsonObject.hasNonNull(key)) {
            return jsonObject.get(key).asBoolean();
        }
        return null;
    }

    public static JsonNode getJSONObject(JsonNode jsonObject, String key) {

        if (jsonObject == null) {
            return null;
        }

        if (jsonObject.hasNonNull(key)) {
            return jsonObject.get(key);
        }
        return null;
    }

    public static String getString(JsonNode jsonObject, String defaultValue, List<String> keys) {
        try {
            for (int i = 0; i < keys.size(); i++) {

                String key = keys.get(i);
                if (!jsonObject.hasNonNull(key)) {
                    return defaultValue;
                }

                if (i == keys.size() - 1) {
                    return getString(jsonObject, key);
                } else {
                    jsonObject = getJSONObject(jsonObject, key);
                }
            }
        } catch (Exception e) {
            logger.info("parse json object error keys = {}, jsonObj = {}", keys, jsonObject, e);
        }

        return defaultValue;
    }

    public static String getString(JsonNode jsonObject, List<String> keys) {
        return getString(jsonObject, null, keys);
    }

}
