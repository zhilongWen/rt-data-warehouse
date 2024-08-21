package com.at.rt.data.warehouse.dwd;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author wenzhilong
 */
public class ParseDBLogFun implements FlatMapFunction<String, JsonNode> {

    @Override
    public void flatMap(String value, Collector<JsonNode> out) {
        try {
            JsonNode jsonNode = new ObjectMapper().readTree(value);
            out.collect(jsonNode);
        } catch (Exception e) {

        }
    }
}
