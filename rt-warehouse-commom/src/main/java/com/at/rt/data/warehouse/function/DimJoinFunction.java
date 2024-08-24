package com.at.rt.data.warehouse.function;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {

    void addDims(T obj, JSONObject dimJsonObj);

    String getTableName();

    String getRowKey(T obj);
}

