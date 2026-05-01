package com.atguigu.multistream;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalJoinExample {

    // 数据结构
    public static class Event {
        public int id;
        public long time;
        public String value;

        public Event() {}

        public Event(int id, long time, String value) {
            this.id = id;
            this.time = time;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Event(" + id + "," + time + "," + value + ")";
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // =========================
        // 1. 构造 left 流
        // =========================
        DataStream<Event> leftStream = env.fromElements(
                new Event(1, 10000L, "L1-id1"),
                new Event(1, 20000L, "L2-id1"),

                new Event(2, 10000L, "L1-id2"),
                new Event(2, 30000L, "L2-id2"),

                new Event(3, 15000L, "L1-id3")
        );

        // =========================
        // 2. 构造 right 流
        // =========================
        DataStream<Event> rightStream = env.fromElements(
                new Event(1, 7000L,  "R1-id1"),
                new Event(1, 12000L, "R2-id1"),
                new Event(1, 25000L, "R3-id1"),

                new Event(2, 8000L,  "R1-id2"),
                new Event(2, 18000L, "R2-id2"),
                new Event(2, 42000L, "R3-id2"),

                new Event(3, 9000L,  "R1-id3"),
                new Event(4, 15000L, "R1-id4")
        );

        // =========================
        // 3. 指定 Watermark（事件时间）
        // =========================
        WatermarkStrategy<Event> wm = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, ts) -> event.time);

        SingleOutputStreamOperator<Event> leftWithWm = leftStream.assignTimestampsAndWatermarks(wm);
        SingleOutputStreamOperator<Event> rightWithWm = rightStream.assignTimestampsAndWatermarks(wm);

        // =========================
        // 4. interval join
        // =========================
        DataStream<String> result = leftWithWm
                .keyBy(e -> e.id)
                .intervalJoin(rightWithWm.keyBy(e -> e.id))
                .between(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(-5),
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(10)
                )
                .process(new ProcessJoinFunction<Event, Event, String>() {
                    @Override
                    public void processElement(Event left,
                                               Event right,
                                               Context ctx,
                                               Collector<String> out) {

                        out.collect("JOIN => " + left + " <--> " + right);
                    }
                });

        result.print();

        env.execute("Interval Join Example");
    }
}