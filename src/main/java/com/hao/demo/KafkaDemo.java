package com.hao.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 文件头部信息
 * ClassName：86185
 */
public class KafkaDemo {


    public static void main(String[] args) {

        Properties props = new Properties();
        //kafka 集群，broker-list C:\Windows\System32\drivers\etc
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092");
        props.put("acks", "all");
        //重试次数
        props.put("retries", 1);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        //序列化
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.hao.demo.MyPartitioner");

        Producer<String,String> producer=new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            //ProducerRecord(String topic, K key, V value)
            producer.send(new ProducerRecord<String, String>("first",
                    "kafka"+Integer.toString(i), "kafka1"+Integer.toString(i)));
        }

        producer.close();
    }
}
