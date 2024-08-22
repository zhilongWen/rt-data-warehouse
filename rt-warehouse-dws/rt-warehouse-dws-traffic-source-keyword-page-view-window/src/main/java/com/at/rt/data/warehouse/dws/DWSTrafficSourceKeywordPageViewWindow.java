package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.StreamExecEnvConf;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 搜索关键词聚合统计
 *
 * @author wenzhilong
 */
public class DWSTrafficSourceKeywordPageViewWindow {

    public static void main(String[] args) throws Exception {

        StreamTableEnvironment tableEnv = StreamExecEnvConf.builderStreamTableEnv(args);

        // 注册自定义函数到表执行环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        String sql = "\n"
                + "SET 'table.exec.resource.default-parallelism' = '5';\n"
                + "SET 'table.exec.source.idle-timeout' = '1000';\n"
                + "\n"
                + "\n"
                + "-- 从页面日志事实表中读取数据 创建动态表  并指定Watermark的生成策略以及提取事件时间字段\n"
                + "create table page_log(\n"
                + "    common map<string,string>,\n"
                + "    page map<string,string>,\n"
                + "    ts bigint,\n"
                + "    et as TO_TIMESTAMP_LTZ(ts, 3),\n"
                + "    WATERMARK FOR et AS et\n"
                + ")WITH (\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'dwd_traffic_page',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092',\n"
                + "    'properties.group.id' = 'DwsTrafficSourceKeywordPageViewWindow',\n"
                + "    'scan.startup.mode' = 'earliest-offset',\n"
                + "    'format' = 'json'\n"
                + ")\n"
                + ";\n"
                + "\n"
                + "-- 过滤出搜索行为\n"
                + "create temporary view search_table as\n"
                + "select \n"
                + "   page['item']  fullword,\n"
                + "   et\n"
                + "from page_log\n"
                + "where page['last_page_id'] = 'search' and page['item_type'] ='keyword' and page['item'] is not null\n"
                + ";\n"
                + "\n"
                + "-- 调用自定义函数完成分词   并和原表的其它字段进行join\n"
                + "create temporary view split_table as\n"
                + "SELECT \n"
                + "    keyword,\n"
                + "    et \n"
                + "FROM search_table,\n"
                + "LATERAL TABLE(ik_analyze(fullword)) t(keyword)\n"
                + ";\n"
                + "\n"
                + "-- sink\n"
                + "create table dws_traffic_source_keyword_page_view_window(\n"
                + "  stt string,   -- 2023-07-11 14:14:14\n"
                + "  edt string, \n"
                + "  cur_date string, \n"
                + "  keyword string, \n"
                + "  keyword_count bigint \n"
                + ")with(\n"
                + " 'connector' = 'doris',\n"
                + " 'fenodes' = '10.211.55.102:7030',\n"
                + "  'table.identifier' = 'gmall_realtime.dws_traffic_source_keyword_page_view_window',\n"
                + "  'username' = 'root',\n"
                + "  'password' = 'root123', \n"
                + "  'sink.properties.format' = 'json', \n"
                + "  'sink.buffer-count' = '400', \n"
                + "  'sink.buffer-size' = '4086',\n"
                + "  'sink.enable-2pc' = 'false',  -- 测试阶段可以关闭两阶段提交,方便测试\n"
                + "  'sink.properties.read_json_by_line' = 'true' \n"
                + ")\n"
                + ";\n"
                + "\n"
                + "-- 分组、开窗、聚合 将聚合的结果写到Doris中\n"
                + "insert into dws_traffic_source_keyword_page_view_window\n"
                + "SELECT \n"
                + "    date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n"
                + "    date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n"
                + "    date_format(window_start, 'yyyy-MM-dd') cur_date,\n"
                + "    keyword,\n"
                + "    count(*) keyword_count\n"
                + "FROM TABLE\n"
                + "(\n"
                + "    TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '1' minute)\n"
                + ")\n"
                + "GROUP BY window_start, window_end,keyword\n"
                + ";\n";


        StreamExecEnvConf.execSQL(tableEnv, sql);

    }
}
