package com.atguigu.repartitions;

import com.atguigu.source.ClickSource;
import com.atguigu.source.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
 
public class ShuffleTest { 
    public static void main(String[] args) throws Exception { 
        // 创建执行环境 
        StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
        //全局并行度设置成2
        env.setParallelism(2);
  // 读取数据源，source并行度不一定受全局并行度控制
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        //stream1算子默认并行是2
       DataStream<String> stream1 = stream.map(x -> x + "1");
        System.out.println(stream1.getParallelism());
        //手动设置stream2算子并行度是4
        DataStream<String> stream2 = stream1.map(x -> x + "1").setParallelism(4);
      System.out.println(stream2.getParallelism());
      stream2.rescale();
      //stream3算子默认并行是4
      DataStream stream3 = stream2.map(x -> x + 1);
    System.out.println(stream3.getParallelism());
        env.execute(); 
    } 
}