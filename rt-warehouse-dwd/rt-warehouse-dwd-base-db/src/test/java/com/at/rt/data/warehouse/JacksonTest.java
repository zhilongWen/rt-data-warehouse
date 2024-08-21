package com.at.rt.data.warehouse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class JacksonTest extends TestCase {

    public void test_3() throws IOException {

        String str = "{\"before\":null,\"after\":{\"source_table\":\"coupon_use\",\"source_type\":\"update\",\"sink_table\":\"dwd_tool_coupon_use\",\"sink_columns\":\"id,coupon_id,user_id,order_id,using_time,used_time,coupon_status\"},\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":0,\"snapshot\":\"false\",\"db\":\"rt_warehouse_conf_db\",\"sequence\":null,\"table\":\"table_process_dwd_conf\",\"server_id\":0,\"gtid\":null,\"file\":\"\",\"pos\":0,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"r\",\"ts_ms\":1724250633640,\"transaction\":null}\n";

        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode jsonNode = objectMapper.readTree(str);
//
//        System.out.println(jsonNode.get("before"));
//        System.out.println(jsonNode.get("after"));
//        System.out.println(jsonNode.get("source"));
//        System.out.println(jsonNode.get("op"));
//        System.out.println(jsonNode.get("ts_ms"));
//        System.out.println(jsonNode.get("transaction"));
//
//
//        System.out.println("---------------");
//        JsonNode jsonNode1 = jsonNode.get("after");
//
//        TableProcessConfDwd tableProcessConfDwd = objectMapper.convertValue(jsonNode1, TableProcessConfDwd.class);
//        System.out.println(tableProcessConfDwd);
//
//        System.out.println(objectMapper.writeValueAsString(tableProcessConfDwd));

        JsonNode jsonNode1 = jsonNode.get("after");
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode1.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            System.out.println(entry.getKey() + " -> " + entry.getValue());
            if (entry.getKey().equals("source_table")) {
                fields.remove();
            }
        }

        System.out.println("===========");
        System.out.println(jsonNode1);

        System.out.println("===========");

        System.out.println(jsonNode1.toString());
    }

    public void test_2() throws IOException {
        System.out.println("111");

        String str = "{  \n"
                + "  \"author\": \"一路向北_Coding\",  \n"
                + "  \"age\": 20,  \n"
                + "  \"hobbies\": [\"coding\", \"leetcode\", \"reading\"]\n"
                + "}\n";

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(str);

        System.out.println(jsonNode.get("author"));
        System.out.println(jsonNode.get("age"));
        System.out.println(jsonNode.get("hobbies"));

        System.out.println("=============");

        JsonNode jsonNode1 = objectMapper.readTree(
                "{\"name\":\"John\", \"age\":30, \"city\":\"New York\"}"
        );

        System.out.println(jsonNode1.get("author").asText());
        System.out.println(jsonNode1.get("age"));
        System.out.println(jsonNode1.get("hobbies"));
        System.out.println(jsonNode1.get("name"));
        System.out.println(jsonNode1.get("city"));
    }

    public void test_1() throws JsonProcessingException {
        System.out.println("111");

        String str = "{  \n"
                + "  \"author\": \"一路向北_Coding\",  \n"
                + "  \"age\": 20,  \n"
                + "  \"hobbies\": [\"coding\", \"leetcode\", \"reading\"]\n"
                + "}\n";

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(str);

        System.out.println(jsonNode.get("author"));
        System.out.println(jsonNode.get("age"));
        System.out.println(jsonNode.get("hobbies"));
    }
}
