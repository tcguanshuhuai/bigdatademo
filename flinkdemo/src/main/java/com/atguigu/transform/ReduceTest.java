package com.atguigu.transform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4),
                Tuple2.of("a", 5)

                );

    stream
        .keyBy(x -> x.f0)
        .reduce(
            (x, y) -> {
              System.out.println(x.f0 +" x is " + x.f1);
              System.out.println(y.f0 + " y is" + y.f1);
              return Tuple2.of(x.f0, x.f1 + y.f1);
            })
        .print();
        env.execute();
    }
}
