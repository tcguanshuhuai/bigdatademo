package com.atguigu.statetest.keyedstate;

import com.atguigu.windowfunctest.Applytest;
import com.atguigu.windowfunctest.Processtest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.time.Duration;
import java.util.Random;

public class ListStateExample {

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
                    ListState<Integer> listState;
                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<>("listState", Integer.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        listState.add(value.f1);
                        if (listState.get().spliterator().getExactSizeIfKnown() == 3) {
                            int count = 0;
                            for (Integer i : listState.get()) {
                                count += i;
                            }
                            listState.clear();
                            out.collect(Tuple2.of(ctx.getCurrentKey(), count/3));
                        }
                    }
                });
        avgs.print();
        env.execute();


    }
}


