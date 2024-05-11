package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class CustomConsumerAutoOffset {
    public static void main(String[] args) {
// 1. 创建kafka 消费者配置类 
        Properties properties = new Properties();
// 2. 添加配置参数 
// 添加连接 
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092");
// 配置序列化 必须 
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 配置消费者组 
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "s454wer3q");

        // 是否自动提交offset 
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 提交offset的时间周期1000ms，默认5s 
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        //3. 创建kafka消费者 
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //4. 设置消费主题  形参是列表 
        consumer.subscribe(Arrays.asList("first"));

        //5. 消费数据 
        while (true) {
            // 读取消息 
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

            // 输出消息 
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.value());
            }
        }
    }
} 