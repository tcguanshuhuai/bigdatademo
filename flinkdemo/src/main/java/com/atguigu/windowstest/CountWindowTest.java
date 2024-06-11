package com.atguigu.windowstest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.time.Duration;

public class CountWindowTest {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
      URL url = CountWindowTest.class.getClassLoader().getResource("input/words3.txt");
      FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(url.toURI())).build();
      DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source");
      source.map(x -> {
                  String[] split = x.split(",");
                  return Tuple2.of(split[0], Long.parseLong(split[1]));
              }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
        .keyBy(x -> x.f0)
        // 设置countwindow,每2个数(相同key)sum1次
        .countWindow(2).sum(1)
        .print();
    env.execute();
  }
}
