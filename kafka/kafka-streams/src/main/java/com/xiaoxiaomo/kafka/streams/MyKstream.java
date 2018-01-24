package com.xiaoxiaomo.kafka.streams;

import com.xiaoxiaomo.kafka.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.String;

/**
 * Created by yangyibo on 16/12/12. my learn demo
 * 高层流DSL
 */
public class MyKstream {
    public static void main(String[] args) {

        //tow instances
        KStreamBuilder instances1 = new KStreamBuilder();
//        filterWordCount(builder);
        lambdaFilter(instances1);
        KStreamBuilder instances2 = new KStreamBuilder();
        lambdaFilter(instances2);

        KafkaStreams ks = new KafkaStreams(instances2, init());
        ks.start();
//        Runtime.getRuntime().addShutdownHook(new Thread(ks::close));
    }

    public static Properties init() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "MyKstream");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.metadata_broker_list);
        properties.setProperty(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, KafkaConfig.zookeeper);
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, String().getClass().getName());
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, String().getClass().getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }


    private static void filterWordCount(KStreamBuilder builder) {
        KStream<String, String> source = builder.stream("topic1");
        KTable<String, Long> count = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String value) {
                return Arrays.asList(value.split(" "));
            }
        }).filter(new Predicate<String, String>() {

            @Override
            public boolean test(String key, String value) {
                if (value.contains("abel")) {
                    return true;
                }
                return false;
            }
        }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {

            public KeyValue<String, String> apply(String key, String value) {

                return new KeyValue<String, String>(value + "--read", value);
            }

        }).groupByKey().count("us");
        count.print();
//        count.to("topic2");
    }

    private static void lambdaFilter(KStreamBuilder builder) {
        KStream<String, String> textLines = builder.stream("topic1");

        textLines
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .map((key, word) -> new KeyValue<>(word, word))
                .filter((k, v) -> (!k.contains("message")))
//              .through("RekeyedIntermediateTopic")
                .groupByKey().count("us").print();
        System.out.println("-----------2-----------");

    }


}
