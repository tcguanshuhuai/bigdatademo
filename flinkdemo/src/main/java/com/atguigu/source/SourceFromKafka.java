package com.atguigu.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SourceFromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置kafka consumer的配置信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node01:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        //add source 表示把kafka当外部的source
        DataStreamSource<String> stream = env.addSource(new
                FlinkKafkaConsumer<String>(
                        "clicks", //订阅哪个topic
                new SimpleStringSchema(), //因为flink处理的message都是需要特定类型的(后面会讲)，所以要把kafka的value转换成flink可以处理的message类型，这块就是定义flink接受的类型
                properties //consumer的properties
        ));
        stream.print();
        env.execute();
    }
}
