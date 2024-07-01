package com.atguigu.statetest.keyedstate;

import com.atguigu.windowfunctest.Processtest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.net.URL;

public class AggregateStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        URL url = Processtest.class.getClassLoader().getResource("input/words10.txt");
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(url.toURI())).build();
        DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source");
        DataStream<Tuple2<String, Integer>> avgs = source.map(x -> {
                    String[] split = x.split(",");
                    return Tuple2.of(split[0], Integer.parseInt(split[1]));
                }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }))
                .keyBy(x -> x.f0).process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                    AggregatingState<Integer, Integer> aggState;
                    ValueState<Integer> countState;

                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("countState", Integer.class));
                        aggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<>("aggregateState", new AggregateFunction<Integer, Integer, Integer>() {
                            @Override
                            public Integer createAccumulator() {
                                return 0;
                            }

                            @Override
                            public Integer add(Integer value, Integer accumulator) {
                                return accumulator + value;
                            }

                            @Override
                            public Integer getResult(Integer accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Integer merge(Integer a, Integer b) {
                                return a + b;
                            }
                        } , Integer.class));
                    }
                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        if (countState.value() == null) {
                            countState.update(0);
                        };
                        Integer count = countState.value() + 1;
                        aggState.add(value.f1);
                        countState.update(count);
                        if (count == 3) {
                            out.collect(Tuple2.of(ctx.getCurrentKey(), aggState.get()/3));
                            countState.clear();
                            aggState.clear();
                        }
                    }
                });
        avgs.print();
        env.execute();


    }
}


