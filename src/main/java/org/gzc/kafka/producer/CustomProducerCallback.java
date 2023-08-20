package org.gzc.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author gzc
 */
public class CustomProducerCallback {
    public static void main(String[] args) {

        // 1 创建kafka生产者对象
        KafkaProducer<String, String> stringStringKafkaProducer = init();


        // 2 发送数据
        for (int i = 0; i < 500; i++) {
            stringStringKafkaProducer.send(new ProducerRecord<>("GZC", "gzc" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        System.out.println("topic is = "+recordMetadata.topic()+" partition = "+ recordMetadata.partition());
                    }

                }
            });
        }

        // 3 关闭资源

        stringStringKafkaProducer.close();
    }

    private static KafkaProducer<String, String> init() {
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");

        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        return new KafkaProducer<>(properties);
    }
}
