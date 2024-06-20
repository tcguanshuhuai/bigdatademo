package com.atguigu.windowfunctest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.time.Duration;

public class TimerTest {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
      URL url = TimerTest.class.getClassLoader().getResource("input/words6.txt");
      FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(url.toURI())).build();
      DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source");
    source
        .map(
            x -> {
              String[] split = x.split(",");
              return Tuple3.of(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            })
        .returns(TypeInformation.of(new TypeHint<Tuple3<String, Long, Integer>>() {}))
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.f1))
        .keyBy(x -> x.f0)
        .process(
            new KeyedProcessFunction<String, Tuple3<String, Long, Integer>, String>() {
              @Override
              public void processElement(
                  Tuple3<String, Long, Integer> value,
                  KeyedProcessFunction<String, Tuple3<String, Long, Integer>, String>.Context ctx,
                  Collector<String> out)
                  throws Exception {

                ctx.timerService().registerEventTimeTimer(10000L);
                out.collect(value.toString());// ctx.timerService().deleteEventTimeTimer(ctx.timestamp() + 3000L);
              }
              @Override
              public void onTimer(
                  long timestamp,
                  KeyedProcessFunction<String, Tuple3<String, Long, Integer>, String>.OnTimerContext
                      ctx,
                  Collector<String> out)
                  throws Exception {
                System.out.println("定时器1触发，时间为：" + timestamp);

              }
            })
        .keyBy(x -> true)
        .process(new KeyedProcessFunction<Boolean, String, String>() {
              @Override
              public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                  ctx.timerService().registerEventTimeTimer(10000L);
                  out.collect(value.toString());
              }
              @Override
              public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                  throws Exception {
                System.out.println("定时器2触发，时间为：" + timestamp);
              }
            })
            .keyBy(x -> true)
            .process(new KeyedProcessFunction<Boolean, String, String>() {
                        @Override
                        public void processElement(String value, Context ctx, Collector<String> out)
                                throws Exception {
                            ctx.timerService().registerEventTimeTimer(10000L);
                        }
                        @Override
                        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                                throws Exception {
                            System.out.println("定时器3触发，时间为：" + timestamp);
                        }
                    })
        .print();
    env.execute();
  }

}
