package com.atguigu.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Trans1 {
    public static void main(String[] args) throws Exception {

        // 1. 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> test1stream = env.fromData(1, 2, 3,4,5);
        DataStream<String> test2stream = test1stream.map(x -> "aa");
        env.execute();


    }
}
