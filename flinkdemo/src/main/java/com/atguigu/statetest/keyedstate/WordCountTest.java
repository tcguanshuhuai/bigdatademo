package com.atguigu.statetest.keyedstate;

import com.atguigu.windowfunctest.Processtest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.net.URISyntaxException;
import java.net.URL;

public class WordCountTest {
  public static void main(String[] args) throws Exception {
    //
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(2);
      URL url = Processtest.class.getClassLoader().getResource("input/words9.txt");
      FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(url.toURI())).build();
      DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source");
      source.keyBy(x -> x).process(new KeyedProcessFunction<String, String, String>() {
          ValueState<Integer> countState;
          @Override
          public void open(OpenContext openContext) throws Exception {
              super.open(openContext);
              countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class));
          }

          @Override
          public void processElement(String key, Context ctx, Collector<String> out) throws Exception {
              Integer count = countState.value();
              if (count == null) {
                  count = 0;
              }
              count++;
              countState.update(count);
              out.collect(key + " : " + countState.value());
          }
      }).print();
      env.execute();
      
  }
}
