package com.atguigu.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class SourceFromFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment
                env
                =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary","./home",1000L));
        clicks.add(new Event("Bob","./cart",2000L));
        DataStream<String> stream = env.readTextFile("input/clicks.txt");
        env.execute();
    }
}
