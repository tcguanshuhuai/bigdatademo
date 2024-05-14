package com.atguigu.sink;

import com.atguigu.source.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L), new Event("Alice", "./prod?id=100", 3000L), new Event("Alice", "./prod?id=200", 3500L), new Event("Bob", "./prod?id=2", 2500L), new Event("Alice", "./prod?id=300", 3600L), new Event("Bob", "./home", 3000L), new Event("Bob", "./prod?id=1", 2300L), new Event("Bob", "./prod?id=3", 3300L));
        String hdfsPath = "hdfs://node01:8020/flink/output";
        FileSink<String> fileSink =
                //我们需要什么才能流式的写一个文件到hdfs呢？
                //1.文件类型：这里是RowFormat
                //2.文件路径：hdfsPath
                //3.文件编码：String + UTF-8
                FileSink.forRowFormat(new Path(hdfsPath), new SimpleStringEncoder<String>("UTF-8"))
                //4.滚动策略(什么时候写完一个文件，开始写下个文件),使用默认的滚动策略并为默认滚动策略配置相应参数
                        // 在写了15分钟或者5分钟没有数据写入或者文件大小达到了1024m，这个文件关闭，则滚动生成新的文件并写入
                        .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(15))
                                .withInactivityInterval(Duration.ofMinutes(5))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                .build()
                )
                .build();
        // 将Event转换成String写入文件 
//        stream.map(Event::toString).sinkTo(fileSink);
        stream.map(Event::toString).print();
        env.execute();
    }
} 