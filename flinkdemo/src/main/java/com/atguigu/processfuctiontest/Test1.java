package com.atguigu.processfuctiontest;

import com.atguigu.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Test1 {
  public static void main(String[] args) throws Exception {
    //
      StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
      DataStreamSource<Event> stream = 
              env.fromElements(
                      new Event("Mary", "./home", 1000L), 
                      new Event("Bob", "./cart", 2000L));
      
      stream.process(new ProcessFunction<Event, String>() {

          @Override
          public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
              out.collect(value.user);
              out.collect(value.url);
          }
      }).print();

      stream.windowAll()

      env.execute();

  }
}
