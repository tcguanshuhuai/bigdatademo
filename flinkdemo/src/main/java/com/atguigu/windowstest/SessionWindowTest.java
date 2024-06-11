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
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.time.Duration;

public class SessionWindowTest {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
      URL url = SessionWindowTest.class.getClassLoader().getResource("input/words2.txt");
      FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(url.toURI())).build();
      DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source");
      source.map(x -> {
                  String[] split = x.split(",");
                  return Tuple2.of(split[0], Long.parseLong(split[1]));
              }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner( (element, recordTimestamp) -> element.f1)
                            )
        .keyBy(x -> x.f0)
        // 设置事件时间+session窗口，如果3s内没有数据过来，那就下个窗口
        .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(3))).process(new MyProcessWindowFunction())
        .print();
    env.execute();
  }

    // 自定义处理窗口函数，输出当前的窗口信息和word count
    public static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>{
        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            //这一步是整个的 wordcount
            Long count = elements.spliterator().getExactSizeIfKnown();
            out.collect("左开右闭" + "窗口" + start + " ~ " + end + "中共有" + count + "个" + key);
        }
    }
}
