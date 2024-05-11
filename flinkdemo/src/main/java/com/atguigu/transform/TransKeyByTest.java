package com.atguigu.transform;

import com.atguigu.source.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransKeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Mary", "./home", 3000L),
                new Event("Bob", "./cart", 2000L));
// 使用Lambda表达式 
        KeyedStream<Event, String> keyedStream = stream.keyBy(e -> e.user);
        env.execute();
    }
}