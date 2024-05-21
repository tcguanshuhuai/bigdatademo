package org.sguan.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class CustomConsumerForTime {

    public static void main(String[] args) {


        Properties properties = new Properties();

        // 连接 
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "node01:9092");

        // key value反序列化 

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test56652");

        // 1 创建一个消费者 
        KafkaConsumer<String, String> kafkaConsumer = new
                KafkaConsumer<>(properties);

        // 2 订阅一个主题 
        ArrayList<String> topics = new ArrayList<>();
        topics.add("testn");
        kafkaConsumer.subscribe(topics);

        Set<TopicPartition> assignment = new HashSet<>();

        while (assignment.size() == 0) {
            kafkaConsumer.poll(Duration.ofSeconds(1));
            // 获取消费者分区分配信息（有了分区分配信息才能开始消费） 
            assignment = kafkaConsumer.assignment();
        }

        HashMap<TopicPartition, Long> timestampToSearch = new
                HashMap<>();

        // 封装集合存储，每个分区对应一天前的数据 
        for (TopicPartition topicPartition : assignment) {
            timestampToSearch.put(topicPartition,
                    System.currentTimeMillis() - 1 * 24 * 3600 * 1000);
        }
        // 获取从1天前开始消费的每个分区的offset  =》通过时间获取offsets
        Map<TopicPartition, OffsetAndTimestamp> offsets =
                kafkaConsumer.offsetsForTimes(timestampToSearch);

        // 遍历每个分区，对每个分区设置offset
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp =
                    offsets.get(topicPartition);

            // 对分区设置offset
            if (offsetAndTimestamp != null) {
                kafkaConsumer.seek(topicPartition,
                        offsetAndTimestamp.offset());
            }
        }
// 3 消费该主题数据 
        while (true) {
            ConsumerRecords<String, String> consumerRecords =
                    kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord :
                    consumerRecords) {
                System.out.println(consumerRecord);
            }
        }
    }
} 