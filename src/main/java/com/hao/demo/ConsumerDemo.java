package com.hao.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 文件头部信息
 * ClassName：86185
 */
public class ConsumerDemo {


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092");
//        props.put("group.id", "test");123
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"test2");
        //重置消费者的offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,  String> consumer  =  new KafkaConsumer<>(props);
        //订阅主题,可订阅多个
        consumer.subscribe(Arrays.asList("first"));
        while (true) {
            //不停获取数据
            ConsumerRecords<String,  String> records  = consumer.poll(100);
            //解析并打印
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(), record.key(), record.value());
        }
    }
}


