package com.xiaoxiaomo.kafka.consumer

/**
  * Created by TangXD on 2018/1/31.
  */

import java.util
import java.util.Properties
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.consumer.ConsumerIterator
import kafka.consumer.KafkaStream
import kafka.javaapi.consumer.ConsumerConnector
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties


/**
  * 自定义消息消费者
  *
  * @author xiaojf 294825811@qq.com
  * @since 2015-7-15 下午11:10:28
  */
object KafkaConsumer1 {
    def main(args: Array[String]): Unit = {
        new KafkaConsumer1().consume()
    }
}

class KafkaConsumer1() {
    val originalProps = new Properties
    //zookeeper 配置，通过zk 可以负载均衡的获取broker
    originalProps.put("zookeeper.connect", "192.168.66.2:2181,192.168.66.3:2181,192.168.66.4:2181")
    //group 代表一个消费组
    originalProps.put("group.id", "hashleaf-group")
    //zk连接超时时间
    originalProps.put("zookeeper.session.timeout.ms", "10000")
    //zk同步时间
    originalProps.put("zookeeper.sync.time.ms", "200")
    //自动提交间隔时间
    originalProps.put("auto.commit.interval.ms", "1000")
    //消息日志自动偏移量,防止宕机后数据无法读取
    originalProps.put("auto.offset.reset", "smallest")
    //序列化类
    originalProps.put("serializer.class", "kafka.serializer.StringEncoder")
    //构建consumer connection 对象
    final private var consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(originalProps))


    def consume(): Unit = { //指定需要订阅的topic
        val topicCountMap = new util.HashMap[String, Integer]
        topicCountMap.put(MyProducer.HASHLEAF_KAFKA_TOPIC, new Integer(5))
        //指定key的编码格式
        val keyDecoder = new StringDecoder(new VerifiableProperties)
        //指定value的编码格式
        val valueDecoder = new StringDecoder(new VerifiableProperties)
        //获取topic 和 接受到的stream 集合
        val map = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder)
        //根据指定的topic 获取 stream 集合
        val kafkaStreams = map.get(MyProducer.HASHLEAF_KAFKA_TOPIC)
        val executor = Executors.newFixedThreadPool(4)
        //因为是多个 message组成 message set ， 所以要对stream 进行拆解遍历
        import scala.collection.JavaConversions._
        for (kafkaStream <- kafkaStreams) {
            executor.submit(new Runnable() {
                override def run(): Unit = { //拆解每个的 stream
                    val iterator = kafkaStream.iterator
                    while ( {
                        iterator.hasNext
                    }) { //messageAndMetadata 包括了 message ， topic ， partition等metadata信息
                        val messageAndMetadata = iterator.next
                        System.out.println("message : " + messageAndMetadata.message + "  partition :  " + messageAndMetadata.partition)
                    }
                }
            })
        }
    }
}
