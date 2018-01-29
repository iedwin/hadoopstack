package com.xiaoxiaomo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 *
 * 消费者
 * 手动提交offset
 *
 * Created by TangXD on 2018/1/8.
 */
public class ConsumerNewWithOffset2 {


    public static void main(String[] args) {
        System.out.println("begin consumer");
        connectionKafka2();
        System.out.println("finish consumer");
    }

    @SuppressWarnings("resource")
    public static void connectionKafka2() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.141.5.22:9092,10.141.5.25:9092,10.141.5.26:9092");
        props.put("group.id", "xxo-test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "100");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");



        String topic = "honeycomb-gw-shebao" ;
//        props.put("auto.offset.reset","earliest"); 重新开始消费



        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {

                System.out.println("在这里批量提交 data：");
                for (ConsumerRecord<String, String> record : buffer) {
                    System.out.println("offset:"+record.offset() +  " key:"+record.key() + "  value"+record.value());
                }

                consumer.commitSync();
                buffer.clear();
            }
        }


    }

}
