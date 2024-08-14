package com.at.rt.data.warehouse.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @author wenzhilong
 */
public class HBaseUtil {

    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    /**
     * 获取Hbase连接
     *
     * @return
     * @throws IOException
     */
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return ConnectionFactory.createConnection(conf);
    }

    /**
     * 关闭Hbase连接
     *
     * @param hbaseConn
     * @throws IOException
     */
    public static void closeHBaseConnection(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    /**
     * 获取异步操作HBase的连接对象
     *
     * @return
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭异步操作HBase的连接对象
     *
     * @param asyncConn
     */
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null && !asyncConn.isClosed()) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 建表
     *
     * @param hbaseConn
     * @param namespace
     * @param tableName
     * @param families
     */
    public static void createHBaseTable(Connection hbaseConn, String namespace, String tableName, String... families) {

        if (families.length < 1) {
            logger.warn("families must nor be empty");
            return;
        }

        try (Admin admin = hbaseConn.getAdmin()) {

            try {
                admin.getNamespaceDescriptor(namespace);
            } catch (NamespaceNotFoundException e) {
                logger.warn("namespace:{} not exists,Trying to create {} ", namespace, namespace);
                admin.createNamespace(NamespaceDescriptor.create(namespace).build());
                logger.info("create namespace:{} success", namespace);
            }

            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObj)) {
                logger.info("namespace:{} has existed table:{}", namespace, tableName);
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }

            admin.createTable(tableDescriptorBuilder.build());

            logger.info("namespace:{} has created table:{}", namespace, tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除表
     *
     * @param hbaseConn
     * @param namespace
     * @param tableName
     */
    public static void dropHBaseTable(Connection hbaseConn, String namespace, String tableName) {

        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            // 判断要删除的表是否存在
            if (!admin.tableExists(tableNameObj)) {
                logger.warn("delete namespace:{} table:{} not exists", namespace, tableName);
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            logger.info("delete namespace:{} table:{} success", namespace, tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 向表中put数据
     *
     * @param hbaseConn 连接对象
     * @param namespace 表空间
     * @param tableName 表名
     * @param rowKey    rowkey
     * @param family    列族
     * @param jsonObj   要put的数据
     */
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObj) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObj.keySet();
            for (String column : columns) {
                String value = jsonObj.getString(column);
                if (StringUtils.isNotEmpty(value)) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向表空间" + namespace + "下的表" + tableName + "中put数据" + rowKey + "成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 从表中删除数据
     *
     * @param hbaseConn
     * @param namespace
     * @param tableName
     * @param rowKey
     */
    public static void delRow(Connection hbaseConn, String namespace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("从表空间" + namespace + "下的表" + tableName + "中删除数据" + rowKey + "成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据rowkey从Hbase表中查询一行数据
     *
     * @param hbaseConn          hbase连接对象
     * @param namespace          表空间
     * @param tableName          表名
     * @param rowKey             rowkey
     * @param clz                将查询的一行数据 封装的类型
     * @param isUnderlineToCamel 是否将下划线转换为驼峰命名
     * @param <T>
     * @return
     */
    public static <T> T getRow(Connection hbaseConn, String namespace, String tableName, String rowKey, Class<T> clz, boolean... isUnderlineToCamel) {

        // 默认不执行下划线转驼峰
        boolean defaultIsUToC = false;

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            if (cells != null && !cells.isEmpty()) {
                //定义一个对象，用于封装查询出来的一行数据
                T obj = clz.newInstance();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if (defaultIsUToC) {
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * 以异步的方式 从HBase维度表中查询维度数据
     *
     * @param asyncConn 异步操作HBase的连接
     * @param namespace 表空间
     * @param tableName 表名
     * @param rowKey    row key
     * @return
     */
    public static JSONObject readDimAsync(AsyncConnection asyncConn, String namespace, String tableName, String rowKey) {
        try {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncConn.getTable(tableNameObj);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if (cells != null && !cells.isEmpty()) {
                JSONObject jsonObj = new JSONObject();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObj.put(columnName, columnValue);
                }
                return jsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
