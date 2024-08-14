package com.at.rt.data.warehouse;

import com.alibaba.fastjson.JSON;
import com.at.rt.data.warehouse.utils.YamlUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YamlUtilTest {


    @Test
    public void test1() {

        System.out.println(JSON.toJSONString(YamlUtil.parseYaml()));

        System.out.println(YamlUtil.getInt("redis.port"));
        System.out.println(YamlUtil.getString("kafka.consumer[app2].server"));

        ParameterTool parameterTool = ParameterTool
                .fromMap(YamlUtil.parseYaml())
                .mergeWith(ParameterTool.fromSystemProperties());

        System.out.println(parameterTool.get("kafka.consumer.app2.server"));
    }

    @Test
    public void test() {

        Map<Object, Object> configMap = YamlUtil.properties;

        System.out.println(configMap);

        Map<String, String> map = new HashMap<>();

//        for (Map.Entry<String, Object> entry : configMap.entrySet()) {
//
//            String key = entry.getKey();
//            Object value = entry.getValue();
//
//            if (value instanceof Map) {
//
//                Map<Object, Object> value1 = (Map<Object, Object>) value;
//                System.out.println(value1);
//
//            }
//
//            System.out.println(value);
//        }

//        parse(configMap, "v");
//
        for (Map.Entry<Object, Object> entry : configMap.entrySet()) {

            String key = String.valueOf(entry.getKey());
            Object value = entry.getValue();

            if (value instanceof Map) {
                parse((Map<Object, Object>) value, key, map);
            } else {
                map.put(key, String.valueOf(value));
            }
        }

        System.out.println("=============================");
        System.out.println(map);

        System.out.println(JSON.toJSONString(map));
    }

    public void parse(Map<Object, Object> configMap, String key, Map<String, String> map) {

        for (Map.Entry<Object, Object> entry : configMap.entrySet()) {

            String kk = key + "." + entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                parse((Map<Object, Object>) value, kk, map);
            } else if (value instanceof List) {

                for (Object v : (List) value) {
                    if (v instanceof Map) {
                        parse((Map<Object, Object>) v, kk, map);
                    }
                }
            } else {
                map.put(kk, String.valueOf(value));
            }
        }

    }

}
