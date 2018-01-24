package com.xiaoxiaomo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;



/**
 * org.apache.kafka.clients 下面的consumer包
 *
 * Created by TangXD on 2017/10/20.
 */
public class ConsumerNew {

    public static void main(String[] args) {
        System.out.println("begin consumer");
        connectionKafka();
        System.out.println("finish consumer");
    }

    @SuppressWarnings("resource")
    public static void connectionKafka() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.141.5.22:9092,10.141.5.25:9092,10.141.5.26:9092");
        props.put("group.id", "xxo-test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

//        props.put("auto.offset.reset","earliest"); 重新开始消费

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


        consumer.subscribe(Arrays.asList("honeycomb-gw-shebao"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                System.out.println();
            }
        }
    }
}
