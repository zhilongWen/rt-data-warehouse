package com.at.rt.data.warehouse.dwd;

import com.at.rt.data.warehouse.bean.TableProcessConfDwd;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author wenzhilong
 */
public class ParseDimBinlogFun implements FlatMapFunction<String, TableProcessConfDwd> {

    @Override
    public void flatMap(String value, Collector<TableProcessConfDwd> out) {

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(value);

            String op = jsonNode.get("op").asText();

            TableProcessConfDwd tableProcessConfDwd;
            if ("d".equals(op)) {
                // 对配置表进行了删除操作   需要从before属性中获取删除前配置信息
                tableProcessConfDwd = objectMapper.convertValue(
                        jsonNode.get("before"), TableProcessConfDwd.class);
            } else {
                // 对配置表进行了读取、插入、更新操作   需要从after属性中获取配置信息
                tableProcessConfDwd = objectMapper.convertValue(
                        jsonNode.get("after"), TableProcessConfDwd.class);
            }

            tableProcessConfDwd.setOp(op);

            out.collect(tableProcessConfDwd);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
