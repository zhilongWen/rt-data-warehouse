package com.at.rt.data.warehouse;

import com.alibaba.fastjson.JSONObject;
import com.at.rt.data.warehouse.bean.TableProcessDim;
import com.at.rt.data.warehouse.utils.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @author wenzhilong
 */
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

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
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) {

        JSONObject jsonObj = value.f0;
        TableProcessDim tableProcessDim = value.f1;
        String type = jsonObj.getString("type");
        jsonObj.remove("type");

        //获取操作的HBase表的表名
        String sinkTable = tableProcessDim.getSinkTable();
        //获取rowkey
        String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());
        //判断对业务数据库维度表进行了什么操作
        if ("delete".equals(type)) {
            //从业务数据库维度表中做了删除操作  需要将HBase维度表中对应的记录也删除掉
            HBaseUtil.delRow(connection, "gmall", sinkTable, rowKey);
        } else {
            //如果不是delete，可能的类型有insert、update、bootstrap-insert，上述操作对应的都是向HBase表中put数据
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(connection, "gmall", sinkTable, rowKey, sinkFamily, jsonObj);
        }
    }
}
