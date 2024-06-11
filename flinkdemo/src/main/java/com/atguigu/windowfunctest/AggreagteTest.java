package com.atguigu.windowfunctest;

import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class AggreagteTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        URL url = AggreagteTest.class.getClassLoader().getResource("input/words4.txt");
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(url.toURI())).build();
        DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source");
        source.map(x -> {
                    String[] split = x.split(",");
                    return Tuple3.of(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                }).returns(TypeInformation.of(new TypeHint<Tuple3<String, Long, Integer>>() {}))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner( (element, recordTimestamp) -> element.f1)
                )
                .keyBy(x -> x.f0)
                // 设置滚动事件时间窗口，窗口长度5s
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //进行avg计算
                .aggregate(new AggregateFunction<Tuple3<String, Long, Integer>, Tuple3<String,Integer, Integer>, Tuple2<String, Integer>>() {

                    @Override
                    public Tuple3<String, Integer, Integer> createAccumulator() {
                        return Tuple3.of("", 0, 0);
                    }

                    @Override
                    public Tuple3<String, Integer, Integer> add(Tuple3<String, Long, Integer> value, Tuple3<String, Integer, Integer> accumulator) {
                        accumulator.f0 = value.f0;
                        accumulator.f1 += value.f2;
                        accumulator.f2++;
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(Tuple3<String, Integer, Integer> accumulator) {
                        if (accumulator.f2 != 0) {
                            return Tuple2.of(accumulator.f0, accumulator.f1/accumulator.f2);
                        }
                        return Tuple2.of(accumulator.f0, 0);
                    }

                    @Override
                    public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> a, Tuple3<String, Integer, Integer> b) {
                        return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
                    }
                })
                .print();
        env.execute();
    }

}

