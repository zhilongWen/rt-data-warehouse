package com.at.rt.data.warehouse;

import com.at.rt.data.warehouse.utils.HBaseUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HBaseUtilTest {

    Connection connection;

    @Before
    public void setUp() throws IOException {
        connection = HBaseUtil.getHBaseConnection();
    }

    @After
    public void tearDown() throws IOException {
        HBaseUtil.closeHBaseConnection(connection);
    }

    @Test
    public void t1() throws Exception{

        System.out.println(connection);

        Admin admin = connection.getAdmin();

        try {
            admin.getNamespaceDescriptor("gmall_test");
        }catch (NamespaceNotFoundException e){
            System.out.println("gmall_test 不存在，试图创建");
            admin.createNamespace(NamespaceDescriptor.create("gmall_test").build());
        }

        TableName tableName = TableName.valueOf("gmall_test", "student");
        if (admin.tableExists(tableName)){
            System.out.println("namespace = gmall_test 下的表 student 以创建");
            return;
        }

        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build()
                )
                .build();

        admin.createTable(tableDescriptor);

    }
}
