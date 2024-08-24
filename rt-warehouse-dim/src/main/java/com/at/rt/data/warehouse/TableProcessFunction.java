package com.at.rt.data.warehouse;

import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.bean.TableProcessDim;
import com.at.rt.data.warehouse.utils.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wenzhilong
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    private final MapStateDescriptor<String, TableProcessDim> dimBroadCastStateDescriptor;

    public TableProcessFunction() {
        this.dimBroadCastStateDescriptor = new MapStateDescriptor<String, TableProcessDim>("dimBroadCastStateDescriptor", String.class, TableProcessDim.class);
    }

    private transient Map<String, TableProcessDim> dimConfigMap;

    @Override
    public void open(Configuration parameters) throws Exception {
        dimConfigMap = new HashMap<>(32);
        //将配置表中的配置信息预加载到程序configMap中
        Connection mySQLConnection = JdbcUtil.getMySQLConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(
                mySQLConnection,
                "select * from rt_warehouse_conf_db.table_process_dim",
                TableProcessDim.class,
                true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            dimConfigMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtil.closeMySQLConnection(mySQLConnection);
    }

    @Override
    public void processElement(JSONObject value,
                               BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx,
                               Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {

        // 主流业务

        // 获取广播流状态
        ReadOnlyBroadcastState<String, TableProcessDim> dimBroadcastState =
                ctx.getBroadcastState(dimBroadCastStateDescriptor);

        // 获取处理的数据的表名
        String table = value.getString("table");

        // 根据表名先到广播状态中获取对应的配置信息
        TableProcessDim tableProcessDim;

        if (!((tableProcessDim = dimBroadcastState.get(table)) != null
                || (tableProcessDim = dimConfigMap.get(table)) != null)) {
            return;
        }

        // 如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据
        // 将维度数据继续向下游传递(只需要传递data属性内容即可)
        JSONObject dataJsonObj = value.getJSONObject("data");

        // 在向下游传递数据前，过滤掉不需要传递的属性
        String sinkColumns = tableProcessDim.getSinkColumns();
        deleteNotNeedColumns(dataJsonObj, sinkColumns);

        //在向下游传递数据前，补充对维度数据的操作类型属性
        String type = value.getString("type");
        dataJsonObj.put("type", type);

        out.collect(Tuple2.of(dataJsonObj, tableProcessDim));

    }

    @Override
    public void processBroadcastElement(TableProcessDim value,
                                        BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx,
                                        Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {

        // 广播数据

        // 获取维度表的操作类型
        String op = value.getOp();
        //获取维度表名称
        String sourceTable = value.getSourceTable();

        // 获取广播流状态
        BroadcastState<String, TableProcessDim> dimBroadcastState = ctx.getBroadcastState(dimBroadCastStateDescriptor);

        if ("d".equals(op)) {
            dimBroadcastState.remove(sourceTable);
            dimConfigMap.remove(sourceTable);
        } else {
            dimBroadcastState.put(sourceTable, value);
            dimConfigMap.put(sourceTable, value);
        }
    }

    /**
     * 过滤掉不需要传递的字段
     * dataJsonObj  {"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"555","id":1}
     * sinkColumns  id,tm_name
     *
     * @param dataJsonObj
     * @param sinkColumns
     */
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        dataJsonObj.entrySet().removeIf(entry -> !columnList.contains(entry.getKey()));
    }
}
