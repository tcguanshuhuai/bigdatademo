package com.atguigu.end2end;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class Kafka2Kafka {
  public static void main(String[] args) {
    //
      KafkaSource<String> source = KafkaSource.<String>builder()
              .setBootstrapServers(brokers)
              .setTopics("input-topic")
              .setGroupId("my-group")
              .setStartingOffsets(OffsetsInitializer.earliest())
              .setValueOnlyDeserializer(new SimpleStringSchema())
              .build();
    
      env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
  }
}
