package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.utils.HBaseUtil;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.hbase.client.Connection;

public class KKl {
    public static void main(String[] args) throws Exception{

        Connection hbaseConn = HBaseUtil.getHBaseConnection();

//        JSONObject row = HBaseUtil.getRow(hbaseConn, "gmall", "dim_activity_info", "1", JSONObject.class);

        ObjectNode row = HBaseUtil.getRow(hbaseConn, "gmall", "dim_activity_info", "1", ObjectNode.class);

        System.out.println(row);

        HBaseUtil.closeHBaseConnection(hbaseConn);
    }
}
