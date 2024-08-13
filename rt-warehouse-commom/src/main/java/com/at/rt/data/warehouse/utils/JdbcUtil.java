package com.at.rt.data.warehouse.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wenzhilong
 */
public class JdbcUtil {

    /**
     * 获取MySQL连接
     *
     * @return Connection
     * @throws Exception
     */
    public static Connection getMySQLConnection() throws Exception {
        //注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        java.sql.Connection conn = DriverManager.getConnection(
                "jdbc:mysql://hadoop102:3306?useSSL=false",
                "root",
                "root123");
        return conn;
    }

    /**
     * 关闭MySQL连接
     *
     * @param conn
     * @throws SQLException
     */
    public static void closeMySQLConnection(Connection conn) throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    /**
     * 从数据库表中查询数据
     *
     * @param conn
     * @param sql
     * @param clz
     * @param isUnderlineToCamel
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz, boolean... isUnderlineToCamel) throws Exception {
        List<T> resList = new ArrayList<>();
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            //通过反射创建一个对象，用于接收查询结果
            T obj = clz.newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                //给对象的属性赋值
                if (defaultIsUToC) {
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }
                BeanUtils.setProperty(obj, columnName, columnValue);
            }
            resList.add(obj);
        }

        return resList;
    }
}
