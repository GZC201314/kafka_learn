package org.gzc.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author gzc
 */
public class CustomProducerCallbackWithCustomPartitioner {
    public static void main(String[] args) {

        // 1 创建kafka生产者对象
        KafkaProducer<String, String> stringStringKafkaProducer = init();


        // 2 发送数据
        for (int i = 0; i < 500; i++) {
            stringStringKafkaProducer.send(new ProducerRecord<>("GZC", "Hello" + i), (recordMetadata, e) -> {
                if (e == null){
                    System.out.println("topic is = "+recordMetadata.topic()+" partition = "+ recordMetadata.partition());
                }

            });
        }

        // 3 关闭资源

        stringStringKafkaProducer.close();
    }

    private static KafkaProducer<String, String> init() {
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.100.2.50:19092,10.100.2.50:19093,10.100.2.50:19094");

        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 在这边设置自定义的Partitioner
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,MyPartitioner.class.getName());

        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");

        return new KafkaProducer<>(properties);
    }
}
