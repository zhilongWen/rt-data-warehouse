package com.at.rt.data.warehouse.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenzhilong
 */
public class DwdBaseLogEtlProcess extends ProcessFunction<String, JSONObject> {

    private static final Logger logger = LoggerFactory.getLogger(DwdBaseLogEtlProcess.class);

    private final OutputTag<String> errorLogTag;

    public DwdBaseLogEtlProcess(OutputTag<String> errorLogTag) {
        this.errorLogTag = errorLogTag;
    }

    @Override
    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

        try {
            out.collect(JSON.parseObject(value));
        } catch (Exception e) {

            logger.warn("dwd base log parse error ", e);

            ctx.output(errorLogTag, value);
        }
    }
}
