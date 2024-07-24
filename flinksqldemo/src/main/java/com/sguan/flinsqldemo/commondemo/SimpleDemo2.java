package com.sguan.flinsqldemo.commondemo;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class SimpleDemo2 {
    public static void main(String[] args) throws Exception{

        // 1. 定义环境配置来创建表
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 2. 创建表
        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.csv', " +
                " 'format' =  'csv' " +
                ")";

        tableEnv.executeSql(createDDL);
        // 3 执行SQL进行表的查询转换
        TableResult tableResult = tableEnv.executeSql("select url, user_name from clickTable");
        Table table = tableEnv.sqlQuery("select url, user_name from clickTable");

        // 4. 创建一张用于输出的表
        String createOutDDL = "CREATE TABLE outTable (" +
                " url STRING, " +
                " user_name STRING " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'output', " +
                " 'format' =  'csv' " +
                ")";
        tableEnv.executeSql(createOutDDL);
        //5. 输出
        tableEnv.executeSql("insert into outTable select url, user_name from clickTable");
    }
}
