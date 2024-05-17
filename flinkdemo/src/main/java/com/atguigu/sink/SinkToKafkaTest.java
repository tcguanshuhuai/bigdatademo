package com.atguigu.sink;

import com.atguigu.source.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkToKafkaTest {
  public static void main(String[] args) throws Exception {
    //
    StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L), new Event("Alice", "./prod?id=100", 3000L), new Event("Alice", "./prod?id=200", 3500L), new Event("Bob", "./prod?id=2", 2500L), new Event("Alice", "./prod?id=300", 3600L), new Event("Bob", "./home", 3000L), new Event("Bob", "./prod?id=1", 2300L), new Event("Bob", "./prod?id=3", 3300L));


    KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers("node01:9092")
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                    .setTopic("clicks")
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    stream.map(x -> x.toString()).sinkTo(sink);
    env.execute();
  }
}
