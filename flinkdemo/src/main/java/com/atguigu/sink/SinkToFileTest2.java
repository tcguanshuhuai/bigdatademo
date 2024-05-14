package com.atguigu.sink;

import com.atguigu.source.Event;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.io.File;
import java.time.Duration;

public class SinkToFileTest2 {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);

    Schema schema =
        new Schema.Parser()
            .parse(
                new File(
                    SinkToFileTest2.class.getClassLoader().getResource("user.avsc").getFile()));
    ;

    GenericRecord user1 = new GenericData.Record(schema);
    user1.put("name", "Alyssa");
    user1.put("favorite_number", 256);
    // Leave favorite color null

    GenericRecord user2 = new GenericData.Record(schema);
    user2.put("name", "Ben");
    user2.put("favorite_number", 7);
    user2.put("favorite_color", "red");

    GenericRecord user3 = new GenericData.Record(schema);
    user3.put("name", "Alyssa1");
    user3.put("favorite_number", 2561);
    // Leave favorite color null

    GenericRecord user4 = new GenericData.Record(schema);
    user4.put("name", "Ben1");
    user4.put("favorite_number", 71);
    user4.put("favorite_color", "red1");

    String hdfsPath = "hdfs://node01:8020/flink/output";

    System.out.println(user1);

    DataStream<GenericRecord> input =
            env.fromData(user1, user2, user3, user4).returns(new GenericRecordAvroTypeInfo(schema));

    final FileSink<GenericRecord> sink =
        FileSink.forBulkFormat(new Path(hdfsPath), AvroParquetWriters.forGenericRecord(schema))
            .build();
    input.sinkTo(sink);
    env.execute();
  }
}
