package org.example.consumer;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerTest1 {
    @SneakyThrows
    public static void main(String[] args) {
        // 0.配置一系列参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092");//kafka集群，broker-list
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test22"); //groupid
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        auto.offset.reset：当消费者读取偏移量无效的情况下，需要重置消费起始位置，
//        默认为latest（从消费者启动后生成的记录），另外一个选项值是 earliest，将从有效的最小位移位置开始消费
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");




        //1.根据参数创建consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //2.订阅topic
        consumer.subscribe(Arrays.asList("first"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value() + "consumer ok");
            }
            Thread.sleep(3000);
        }
    }
}