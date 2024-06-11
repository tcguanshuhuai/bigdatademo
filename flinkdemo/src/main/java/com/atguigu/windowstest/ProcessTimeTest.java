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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class ProcessTimeTest {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
      env.socketTextStream("localhost", 7777)
        .keyBy(x -> x)
        // 设置处理时间滚动窗口，窗口长度60s
        .window(TumblingProcessingTimeWindows.of(Time.seconds(60))).process(new MyProcessWindowFunction())

        .print();
    env.execute();
  }

    // 自定义处理窗口函数，输出当前的窗口信息和word count
    public static class MyProcessWindowFunction extends ProcessWindowFunction<String, String, String, TimeWindow>{
        @Override
        public void process(String key, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            //这一步是整个的 wordcount
            Long count = elements.spliterator().getExactSizeIfKnown();

            long currentTimeStamp = System.currentTimeMillis();

            LocalDateTime startTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(start), ZoneId.systemDefault());
            LocalDateTime endTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(end), ZoneId.systemDefault());
            LocalDateTime currentTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(currentTimeStamp), ZoneId.systemDefault());

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");


            out.collect("左开右闭" + "窗口" + startTime.format(formatter) + " ~ " + endTime.format(formatter) + "中共有" + count + "个" + key + "， 现在时间：" + currentTime.format(formatter));
        }
    }
}
