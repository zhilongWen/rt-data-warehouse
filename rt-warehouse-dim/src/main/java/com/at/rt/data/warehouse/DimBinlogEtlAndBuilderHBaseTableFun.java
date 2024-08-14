package com.at.rt.data.warehouse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.bean.TableProcessDim;
import com.at.rt.data.warehouse.utils.HBaseUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @author wenzhilong
 */
public class DimBinlogEtlAndBuilderHBaseTableFun extends RichFlatMapFunction<String, TableProcessDim> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(connection);
    }

    @Override
    public void flatMap(String value, Collector<TableProcessDim> out) throws Exception {

        JSONObject jsonObject = JSON.parseObject(value);

        String op = jsonObject.getString("op");

        TableProcessDim dim;

        if ("d".equals(op)) {
            dim = jsonObject.getObject("before", TableProcessDim.class);
            deleteTable(dim);
            dim.setOp(op);
        } else if ("c".equals(op) || "r".equals(op)) {
            dim = jsonObject.getObject("after", TableProcessDim.class);
            createTable(dim);
            dim.setOp(op);
        } else {
            dim = jsonObject.getObject("after", TableProcessDim.class);
            deleteTable(dim);
            createTable(dim);
            dim.setOp(op);
        }

        out.collect(dim);
    }

    private void createTable(TableProcessDim dim) {
        HBaseUtil.createHBaseTable(
                connection,
                "gmall",
                dim.getSinkTable(),
                dim.getSinkFamily().split(",")
        );
    }

    private void deleteTable(TableProcessDim dim) {
        HBaseUtil.dropHBaseTable(connection, "gmall", dim.getSinkTable());
    }
}
