package com.sguan.flinsqldemo.commondemo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FromHdfsToKafka   {

    // 1. 定义环境配置来创建表
    EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .build();

    TableEnvironment tableEnv = TableEnvironment.create(settings);
}
