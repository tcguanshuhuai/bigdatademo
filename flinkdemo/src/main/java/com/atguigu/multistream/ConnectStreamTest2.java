package com.atguigu.multistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class ConnectStreamTest2 {
  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // 来自app的支付日志
    SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream =
        env.fromElements(Tuple3.of("order-1", "app", 1000L), Tuple3.of("order-2", "app", 2000L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                          @Override
                          public long extractTimestamp(
                              Tuple3<String, String, Long> element, long recordTimestamp) {
                            return element.f2;
                          }
                        }));

    // 来自第三方支付平台的支付日志
    SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdpartStream =
        env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                          @Override
                          public long extractTimestamp(
                              Tuple4<String, String, String, Long> element, long recordTimestamp) {
                            return element.f3;
                          }
                        }));

    // 合并两条流
      ConnectedStreams<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>> connect = appStream.connect(thirdpartStream);
      ConnectedStreams<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>> keyBy = connect.keyBy(data -> data.f0, data -> data.f0);
      keyBy.process(new KeyedCoProcessFunction<Object, Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, Object>() {
          @Override
          public void processElement1(Tuple3<String, String, Long> value, KeyedCoProcessFunction<Object, Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, Object>.Context ctx, Collector<Object> out) throws Exception {

          }

          @Override
          public void processElement2(Tuple4<String, String, String, Long> value, KeyedCoProcessFunction<Object, Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, Object>.Context ctx, Collector<Object> out) throws Exception {

          }
      });


      //
  }
}
