package com.at.rt.data.warehouse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenzhilong
 */
public class MaxwellLogEtl extends RichFlatMapFunction<String, JSONObject> {

    private static final Logger logger = LoggerFactory.getLogger(MaxwellLogEtl.class);

    @Override
    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
        try {
            JSONObject jsonObject = JSON.parseObject(value);

            String database = jsonObject.getString("database");
            String type = jsonObject.getString("type");
            JSONObject data = jsonObject.getJSONObject("data");

            if ("gmall".equals(database) &&
                    !"bootstrap-start".equals(type) &&
                    !"bootstrap-complete".equals(type) &&
                    data != null &&
                    !data.isEmpty()) {
                out.collect(jsonObject);
            }
        } catch (Exception e) {
            logger.warn("parse log error log: {}", value, e);
        }
    }
}
