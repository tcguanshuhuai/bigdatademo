package com.atguigu.latetest;

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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.time.Duration;

public class LatenessWithProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        URL url = LatenessWithProcess.class.getClassLoader().getResource("input/words5.txt");
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(url.toURI())).build();
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        source.map(x -> {
                    String[] split = x.split(",");
                    return Tuple3.of(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                }).returns(TypeInformation.of(new TypeHint<Tuple3<String, Long, Integer>>() {}))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner( (element, recordTimestamp) -> element.f1)
                )
                .keyBy(x -> x.f0)
                // 设置滚动事件时间窗口，窗口长度5s
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                // 设置允许延迟时间，延迟2s
                .allowedLateness(Duration.ofSeconds(2))
                //进行sum计算
                .process(new ProcessWindowFunction<Tuple3<String, Long, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple3<String, Long, Integer>> input, Collector<String> out) throws Exception {
                        Long start = context.window().getStart();
                        Long end = context.window().getEnd();
                        Long currentWatermark = context.currentWatermark();
                        int sum = 0;
                        for (Tuple3<String, Long, Integer> tuple3 : input) {
                            sum += tuple3.f2;
                        }
                        String notify = "左闭右开窗口" + start + " ~ " + end + "中key为" + key + "的sum值是" + sum + "，窗口闭合计算时，水位线处于：" + currentWatermark;
                        out.collect(notify);

                    }
                })
                .print();
        env.execute();
    }

}

