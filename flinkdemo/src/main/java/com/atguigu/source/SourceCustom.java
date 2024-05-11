package com.atguigu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        //有了自定义的source function，调用addSource方法
        DataStreamSource<Event> stream = env.addSource(new ClickSource()).setParallelism(2);

        stream.print("SourceCustom");

        env.execute();

    }


} 