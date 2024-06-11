//package com.atguigu.windowfunctest;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.connector.file.src.FileSource;
//import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//
//import java.net.URL;
//import java.time.Duration;
//
//public class Processfunctiontest {
//  public static void main(String[] args) throws Exception {
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.setParallelism(2);
//      URL url = Processfunctiontest.class.getClassLoader().getResource("input/words4.txt");
//      FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(url.toURI())).build();
//      DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source");
//      source.map(x -> {
//                  String[] split = x.split(",");
//                  return Tuple3.of(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
//              }).returns(TypeInformation.of(new TypeHint<Tuple3<String, Long, Integer>>() {}))
//            .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner( (element, recordTimestamp) -> element.f1)
//                            )
//        .keyBy(x -> x.f0)
//        // 设置滚动事件时间窗口，窗口长度5s
//        .window(TumblingEventTimeWindows.of(Time.seconds(5))).apply()
//              //进行sum计算
//              .reduce((x, y) -> Tuple3.of(x.f0, -1l, x.f2 + y.f2))
//              .map(x -> Tuple2.of(x.f0, x.f2))
//              .returns(new TypeHint<Tuple2<String, Integer>>() {
//              })
//              .print();
//    env.execute();
//  }
//
//}
