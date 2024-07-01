package com.atguigu.statetest.operatorstate;

import com.atguigu.windowfunctest.Processtest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.net.URL;

public class OperatorStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        URL url = Processtest.class.getClassLoader().getResource("input/words10.txt");
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(url.toURI())).build();
        DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source");
        DataStream<Tuple2<String, Integer>> keyedStream = source.map(x -> {
                    String[] split = x.split(",");
                    return Tuple2.of(split[0], Integer.parseInt(split[1]));
                }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                })).keyBy(x -> x.f0).map(x -> x).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));
    DataStream<Tuple2<String, Integer>> avgs =
        keyedStream.process(
            new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
              ValueState<Integer> count;

              @Override
              public void open(OpenContext openContext) throws Exception {
                count =
                    getRuntimeContext()
                        .getState(new ValueStateDescriptor<Integer>("count", Integer.class));
              }

              @Override
              public void processElement(
                  Tuple2<String, Integer> value,
                  ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx,
                  Collector<Tuple2<String, Integer>> out)
                  throws Exception {
                if (count.value() == null) {
                  count.update(0);
                }
                count.update(count.value() + 1);
                  System.out.println(count.value());
              }
            });
        avgs.print();
        env.execute();


    }
}


