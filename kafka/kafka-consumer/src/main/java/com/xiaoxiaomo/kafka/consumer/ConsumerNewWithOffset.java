package com.xiaoxiaomo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;


/**
 *
 * 消费者
 *
 * 指定最大消费条数：max.poll.records
 * kafka在0.9版本无max.poll.records参数，默认拉取记录是500，直到0.10版本才引入该参数，所以在0.9版本配置是无效的。
 *
 * 手动提交offset
 *
 * Created by TangXD on 2018/1/8.
 */
public class ConsumerNewWithOffset {


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
        props.put("max.poll.records","300"); // 最大300条消息


        String topic = "honeycomb-gw-shebao" ;
//        props.put("auto.offset.reset","earliest"); 重新开始消费



        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            System.out.println("============== 在这里批量获取 data ===========");
            for (ConsumerRecord<String, String> record : records) {
                System.out.print("offset:"+record.offset() +  " ");
//                System.out.println("offset:"+record.offset() +  " key:"+record.key() + "  value"+record.value());
            }
            System.out.println();
            consumer.commitSync();
        }


    }

}
