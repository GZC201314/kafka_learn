package org.gzc.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * @author gzc
 */
public class CustomProducerTranactions {
    public static void main(String[] args) {

        // 1 创建kafka生产者对象
        KafkaProducer<String, String> stringStringKafkaProducer = init();


        // 初始化事物
        stringStringKafkaProducer.initTransactions();

        // 开启事物

        stringStringKafkaProducer.beginTransaction();

        try{
            // 2 发送数据
            for (int i = 0; i < 5; i++) {
                stringStringKafkaProducer.send(new ProducerRecord<>("GZC", "Hello" + i));
            }
            stringStringKafkaProducer.commitTransaction();
        }catch (Exception e){
            stringStringKafkaProducer.abortTransaction();
        }finally {

            // 3 关闭资源
            stringStringKafkaProducer.close();
        }



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

        // 配置 acks ps 这个只能是String类型，不能说Integer类型
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");

        // 设置事物ID 开启事物时，一定要设置事物ID
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());

        return new KafkaProducer<>(properties);
    }
}
