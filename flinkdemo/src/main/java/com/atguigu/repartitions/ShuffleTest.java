package com.atguigu.repartitions;

import com.atguigu.source.ClickSource;
import com.atguigu.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment; 
 
public class ShuffleTest { 
    public static void main(String[] args) throws Exception { 
        // 创建执行环境 
        StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment(); 
        env.setParallelism(1); 
 
  // 读取数据源，并行度为1 
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.shuffle()
 
  // 经洗牌后打印输出，并行度为4 
        stream.shuffle().print("shuffle").setParallelism(4); 
 
        env.execute(); 
    } 
} 