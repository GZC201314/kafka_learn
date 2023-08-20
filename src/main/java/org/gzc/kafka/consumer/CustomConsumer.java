package org.gzc.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gzc.kafka.repository.ConsumerMessage;
import org.gzc.kafka.repository.LiveMessage;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class CustomConsumer {


    public static void main(String[] args) {


        String topicName = "second";

        Properties properties = new Properties();
        // 连接集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.100.2.50:19092,10.100.2.50:19093,10.100.2.50:19094");

        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");


        //1. 创建一个消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());


        //2.订阅主题
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        List<TopicPartition> topicPartitions = Collections.singletonList(topicPartition);
        kafkaConsumer.assign(topicPartitions);

        Long endOffset = kafkaConsumer.endOffsets(topicPartitions).get(topicPartition);
        kafkaConsumer.seek(topicPartition, endOffset);
        // 消费数据
        while (true) {
            List<ConsumerRecord<String, String>> records = new ArrayList<>();

            List<ConsumerRecord<String, String>> polled = kafkaConsumer.poll(Duration.ofMillis(200)).records(topicPartition);


            if (!(polled!= null && polled.isEmpty())) {

                for (ConsumerRecord<String, String> consumerRecord : polled) {
                    records.add(consumerRecord);
                }
            }

            List<ConsumerMessage> data = records
                    .stream()
                    .map(record -> {
                        System.out.println(record);
                        int partition = record.partition();
                        long timestamp = record.timestamp();
                        String key = record.key();
                        String value = record.value();
                        long offset = record.offset();

                        ConsumerMessage consumerMessage = new ConsumerMessage();
                        consumerMessage.setTopic(topicName);
                        consumerMessage.setOffset(offset);
                        consumerMessage.setPartition(partition);
                        consumerMessage.setTimestamp(timestamp);
                        consumerMessage.setKey(key);
                        consumerMessage.setValue(value);

                        return consumerMessage;
                    }).collect(Collectors.toList());

            Long currBeginningOffset = kafkaConsumer.beginningOffsets(topicPartitions).get(topicPartition);
            Long currEndOffset = kafkaConsumer.endOffsets(topicPartitions).get(topicPartition);

            LiveMessage liveMessage = new LiveMessage();
            liveMessage.setBeginningOffset(currBeginningOffset);
            liveMessage.setEndOffset(currEndOffset);
            liveMessage.setPartition(0);
            liveMessage.setMessages(data);


        }
    }
}
