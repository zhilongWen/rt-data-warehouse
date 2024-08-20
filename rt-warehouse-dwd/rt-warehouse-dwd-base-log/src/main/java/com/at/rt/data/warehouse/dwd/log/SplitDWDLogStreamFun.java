package com.at.rt.data.warehouse.dwd.log;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author wenzhilong
 */
public class SplitDWDLogStreamFun extends ProcessFunction<JSONObject, String> {

    OutputTag<String> errTag = new OutputTag<String>("errTag") {
    };
    OutputTag<String> startTag = new OutputTag<String>("startTag") {
    };
    OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
    };
    OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
    };

    @Override
    public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) {

        //~~~错误日志~~~
        JSONObject errJsonObj = value.getJSONObject("err");
        if (errJsonObj != null) {
            //将错误日志写到错误侧输出流
            ctx.output(errTag, value.toJSONString());
            value.remove("err");
        }

        JSONObject startJsonObj = value.getJSONObject("start");
        if (startJsonObj != null) {
            //~~~启动日志~~~
            //将启动日志写到启动侧输出流
            ctx.output(startTag, value.toJSONString());
        } else {
            //~~~页面日志~~~
            JSONObject commonJsonObj = value.getJSONObject("common");
            JSONObject pageJsonObj = value.getJSONObject("page");
            Long ts = value.getLong("ts");

            //~~~曝光日志~~~
            JSONArray displayArr = value.getJSONArray("displays");
            if (displayArr != null && !displayArr.isEmpty()) {
                //遍历当前页面的所有曝光信息
                for (int i = 0; i < displayArr.size(); i++) {
                    JSONObject dispalyJsonObj = displayArr.getJSONObject(i);
                    //定义一个新的JSON对象，用于封装遍历出来的曝光数据
                    JSONObject newDisplayJsonObj = new JSONObject();
                    newDisplayJsonObj.put("common", commonJsonObj);
                    newDisplayJsonObj.put("page", pageJsonObj);
                    newDisplayJsonObj.put("display", dispalyJsonObj);
                    newDisplayJsonObj.put("ts", ts);
                    //将曝光日志写到曝光侧输出流
                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                }
                value.remove("displays");
            }

            //~~~动作日志~~~
            JSONArray actionArr = value.getJSONArray("actions");
            if (actionArr != null && !actionArr.isEmpty()) {
                //遍历出每一个动作
                for (int i = 0; i < actionArr.size(); i++) {
                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                    //定义一个新的JSON对象，用于封装动作信息
                    JSONObject newActionJsonObj = new JSONObject();
                    newActionJsonObj.put("common", commonJsonObj);
                    newActionJsonObj.put("page", pageJsonObj);
                    newActionJsonObj.put("action", actionJsonObj);
                    //将动作日志写到动作侧输出流
                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                }
                value.remove("actions");
            }

            //页面日志  写到主流中
            out.collect(value.toJSONString());
        }
    }
}
