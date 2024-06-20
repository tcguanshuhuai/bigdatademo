package com.atguigu.multistream;

import com.atguigu.windowstest.SessionWindowTest;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.time.Duration;

public class WindowJoinExample {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

      URL url = SessionWindowTest.class.getClassLoader().getResource("input/words7.txt");
      FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(url.toURI())).build();
      DataStream<Tuple3<String, Long, String>> stream1 = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source").map(x -> {
                  String[] split = x.split(",");
                  return Tuple3.of(split[0], Long.parseLong(split[1]), split[2]);
              }).returns(TypeInformation.of(new TypeHint<Tuple3<String, Long, String>>() {
              }))
              .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner((element, recordTimestamp) -> element.f1)
              );

      URL url2 = SessionWindowTest.class.getClassLoader().getResource("input/words8.txt");
      FileSource<String> fileSource2 = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(url2.toURI())).build();
      DataStream<Tuple3<String, Long, String>> stream2 = env.fromSource(fileSource2, WatermarkStrategy.noWatermarks(), "File Source").map(x -> {
                  String[] split = x.split(",");
                  return Tuple3.of(split[0], Long.parseLong(split[1]), split[2]);
              }).returns(TypeInformation.of(new TypeHint<Tuple3<String, Long, String>>() {
              }))
              .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner((element, recordTimestamp) -> element.f1)
              );

      stream1
        .join(stream2)
        .where(r -> r.f0)
        .equalTo(r -> r.f0)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
              .apply(new JoinFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>, Tuple5<String, Long, String, Long, String>>() {
                  @Override
                  public Tuple5<String, Long, String, Long, String> join(Tuple3<String, Long, String> first, Tuple3<String, Long, String> second) throws Exception {
                      return  new Tuple5<String, Long, String, Long, String>(first.f0, first.f1, first.f2, second.f1, second.f2);
                  }
              }).process(new ProcessFunction<Tuple5<String, Long, String, Long, String>, String>() {
                  @Override
                  public void processElement(Tuple5<String, Long, String, Long, String> value, Context context, Collector<String> out) throws Exception {

                      out.collect(value.toString());
                  }
              })
        .print();

    env.execute();
  }
}


































































































































































































































































































































































































































































































































































































































































































































































































































































