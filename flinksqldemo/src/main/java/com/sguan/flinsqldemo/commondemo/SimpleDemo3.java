package com.sguan.flinsqldemo.commondemo;

import com.sguan.flinsqldemo.source.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SimpleDemo3 {
    public static void main(String[] args) throws Exception {
        // 获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // data stream --> table object
        Table tabletest = tableEnv.fromDataStream(eventStream);
        //table object --> view
        tableEnv.createTemporaryView("EventTable", tabletest);

        // data stream --> view
        tableEnv.createTemporaryView("EventTable", eventStream);



        // view --> table object
        // 查询Alice的访问url列表
        Table aliceVisitTable = tableEnv.sqlQuery("SELECT url, user FROM EventTable WHERE user = 'Alice'");

        // 统计每个用户的点击次数
        Table urlCountTable = tableEnv.sqlQuery("SELECT user, COUNT(url) FROM EventTable GROUP BY user");

        // 将表转换成数据流，在控制台打印输出
        //table object --> data stream
        tableEnv.toDataStream(aliceVisitTable).print("alice visit");
        tableEnv.toChangelogStream(urlCountTable).print("count");

        // 执行程序
        env.execute();
    }

}
