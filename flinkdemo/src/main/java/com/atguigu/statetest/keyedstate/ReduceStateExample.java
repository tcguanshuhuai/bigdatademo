package com.atguigu.statetest.keyedstate;

import com.atguigu.windowfunctest.Processtest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

public class ReduceStateExample {

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

                    ReducingState<Integer> reduceState;
                    ValueState<Integer> countState;

                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        reduceState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("reduceState", new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                return value1 + value2;
                            }
                        }, Integer.class));
                        countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("countState", Integer.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        if (countState.value() == null) {
                            countState.update(0);
                        };
                        Integer count = countState.value() + 1;
                        reduceState.add(value.f1);
                        countState.update(count);
                        if (count == 3) {
                            out.collect(Tuple2.of(ctx.getCurrentKey(), reduceState.get()/3));
                            countState.clear();
                            reduceState.clear();
                        }
                    }
                });
        avgs.print();
        env.execute();


    }
}


