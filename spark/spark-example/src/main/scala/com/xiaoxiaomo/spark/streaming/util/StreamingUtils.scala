package com.xiaoxiaomo.spark.streaming.util

import java.util.Properties

import com.xiaoxiaomo.utils.kafka.KafkaCluster
import com.xiaoxiaomo.utils.zookeeper.ZKStringSerializer
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  *
  * Created by TangXD on 2016/11/10.
  */
object StreamingUtils {

    private val LOG: Logger = LoggerFactory.getLogger(StreamingUtils.getClass)
    val properties: Properties = createProperties

    // 加载配置
    def createProperties: Properties = {
        val properties: Properties = new Properties()
        properties.load(this.getClass().getResourceAsStream("/kafka.properties"))
        properties
    }

    // 创建Kafka持续读取流，通过zk中记录的offset
    def createDirectStream( ssc: StreamingContext ,group:String ,topics: String ): InputDStream[(String, String)]  =  {
        val kafkaParams = StreamingUtils.getKafkaParams( group )
        val fromOffsets = StreamingUtils.getFromOffsets( group ,topics )
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }

    // zk 链接
    def zkClient: ZkClient = {
        new ZkClient(properties.getProperty("zk.connect"), Integer.MAX_VALUE, 100000, ZKStringSerializer)
    }


    // kafka 参数
    def getKafkaParams(groupName: String): scala.collection.immutable.Map[String, String] = {
        scala.collection.immutable.Map[String, String](
            "metadata.broker.list" -> properties.getProperty("bootstrap.servers"),
            "group.id" -> groupName,
            "auto.offset.reset" -> "largest")
    }

    // SparkContext
    def getSparkContext( appName:String, maxRatePerPartition:String): SparkContext = {
        //初始化配置
        val sparkConf: SparkConf = new SparkConf()
                .setAppName(appName)
                .set("spark.yarn.am.memory", properties.getProperty("am.memory"))
                .set("spark.yarn.am.memoryOverhead", properties.getProperty("am.memoryOverhead"))
                .set("spark.yarn.executor.memoryOverhead", properties.getProperty("executor.memoryOverhead"))
                .set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.reducer.maxSizeInFlight", "1m")
                .set("spark.scheduler.mode", "FAIR")
                .set("spark.locality.wait", "100ms")

        new SparkContext(sparkConf)
    }

    // StreamingContext
    def getStreamingContext( sc: SparkContext, timeWindow: Int ): StreamingContext = {
        new StreamingContext(sc, Seconds(timeWindow.toInt)) //多少秒处理一次请求
    }


    // HiveContext
    def getHiveContext(sc: SparkContext): HiveContext = {
        val hiveContext: HiveContext = new HiveContext(sc)
        hiveContext.setConf("mapred.reduce.tasks","5")
        hiveContext.setConf("spark.sql.shuffle.partitions","10")
        hiveContext
    }


    // fromOffsets
    def getFromOffsets( groupName:String ,kafka_topics: String ): Map[TopicAndPartition, Long] ={
        val topics: Set[String] = kafka_topics.split(",").toSet
        var fromOffsets: Map[TopicAndPartition, Long] = Map() //多个partition的offset

        //支持多个topic : Set[String]
        topics.foreach( topicName => {

            //去brokers中获取partition数量，注意：新增partition后需要重启
            val children = zkClient.countChildren(ZkUtils.getTopicPartitionsPath(topicName))
            for (i <- 0 until children) {

                //kafka consumer 中是否有该partition的消费记录，如果没有设置为0
                val tp = TopicAndPartition(topicName, i)
                val path: String = s"${new ZKGroupTopicDirs(groupName, topicName).consumerOffsetDir}/$i"
                if (zkClient.exists(path)) {
                    fromOffsets += (tp -> zkClient.readData[String](path).toLong)
                } else {
                    fromOffsets += (tp -> 0)
                }
            }
        })
        LOG.info(s"+++++++++++++++++++ fromOffsets $fromOffsets +++++++++++++++++++++++++ ")
        return fromOffsets
    }


    // updateZkOffset
    def updateZkOffset(groupName: String , offsetRange: OffsetRange): Unit ={
        val kc = new KafkaCluster( getKafkaParams(groupName) )
        //TopicAndPartition 主构造参数第一个是topic，第二个是Kafka partition id
        val topicAndPartition = TopicAndPartition(offsetRange.topic, offsetRange.partition)
        val either = kc.setConsumerOffsets(groupName, Map((topicAndPartition, offsetRange.untilOffset))) //是
        if (either.isLeft) {
            LOG.info(s"Error updating the offset to Kafka cluster: ${either.left.get}")
        }
        LOG.debug(s"${offsetRange.topic} ${offsetRange.partition} ${offsetRange.fromOffset} ${offsetRange.untilOffset}")
    }

}
