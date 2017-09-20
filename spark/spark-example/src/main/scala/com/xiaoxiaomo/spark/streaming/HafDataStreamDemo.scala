package com.xiaoxiaomo.spark.streaming

import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.xiaoxiaomo.utils.common.{KafkaCluster, ZKStringSerializer}
import com.xiaoxiaomo.utils.common.KafkaCluster
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.Seq
import scala.util.control.Breaks
import scala.collection.Seq

/**
  * spark consumer 消费kafka数据 然后计算
  *
  * 运行示例，具体见配置文件：
  * /opt/cloudera/parcels/CDH/bin/spark-submit --master yarn-client --class com.creditease.spark.streaming.HafDataStreamDemo .jar 3 1000
  *
  */
object HafDataStreamDemo {

    private val logger: Logger = LoggerFactory.getLogger(HafDataStreamDemo.getClass)

    def main(args: Array[String]) {

        //接收参数
        val Array(timeWindow, maxRatePerPartition) = args

        //加载配置
        val prop: Properties = new Properties()
        prop.load(this.getClass().getResourceAsStream("/kafka.properties"))

        val groupName = prop.getProperty("haf.group.id")

        //获取配置文件中的topic
        val kafkaTopics: String = prop.getProperty("haf.kafka.topic.bill")
        if (kafkaTopics == null || kafkaTopics.length <= 0) {
            System.err.println("Usage: HafDataStream <kafka_topic> is number from kafka.properties")
            System.exit(1)
        }

        val topics: Set[String] = kafkaTopics.split(",").toSet

        val kafkaParams = scala.collection.immutable.Map[String, String](
            "metadata.broker.list" -> prop.getProperty("bootstrap.servers"),
            "group.id" -> groupName,
            "auto.offset.reset" -> "largest")

        val kc = new KafkaCluster(kafkaParams)

        //初始化配置
        @transient
        val sparkConf = new SparkConf()
                .setAppName(HafDataStreamDemo.getClass.getSimpleName + topics.toString())
                .set("spark.yarn.am.memory", prop.getProperty("am.memory"))
                .set("spark.yarn.am.memoryOverhead", prop.getProperty("am.memoryOverhead"))
                .set("spark.yarn.executor.memoryOverhead", prop.getProperty("executor.memoryOverhead"))
                .set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition) //此处为每秒每个partition的条数
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.reducer.maxSizeInFlight", "1m")
                .set("spark.scheduler.mode", "FAIR")
                .set("spark.locality.wait", "100ms")
        @transient
        val sc = new SparkContext(sparkConf)
        val sql: SQLContext = new SQLContext(sc)
        val ssc = new StreamingContext(sc, Seconds(timeWindow.toInt)) //多少秒处理一次请求

        //zk
        val zkClient = new ZkClient(prop.getProperty("zk.connect"), Integer.MAX_VALUE, 100000, ZKStringSerializer)
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())


        var fromOffsets: Map[TopicAndPartition, Long] = Map() //多个partition的offset


        //支持多个topic : Set[String]
        topics.foreach(topicName => {

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

        logger.info(s"+++++++++++++++++++ fromOffsets $fromOffsets. +++++++++++++++++++++++++ ")
        //创建Kafka持续读取流，通过zk中记录的offset
        val messages: InputDStream[(String, String)] =
            KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

        //数据操作，解析

        var i=0
        //rdd就是所有的数据
        messages.foreachRDD( rdd => {

            val data = rdd.map( x=> (x._1,(JSON.parseObject(x._2).getJSONArray("q"),JSON.parseObject(x._2).getJSONArray("v")) ) ).groupByKey()


            data.map( value => {

                val schema = new StructType()
//                Breaks.breakable{
//                    value._2.foreach(x => {
//                        val q = x._1.iterator()
//                        // 遍历解析schema
//                        while ( q.hasNext ){
//                            schema.add(StructField(q.next().toString, StringType, true))
//                        }
//                        Breaks
//                    } )
//
//                }

                schema.add(StructField("aa".toString, StringType, true))
                schema.add(StructField("bb".toString, StringType, true))
                schema.add(StructField("cc".toString, StringType, true))

                val rows = new util.ArrayList[Row]()
                val rowRDD = value._2.map(x => {
                    rows.add(Row.fromSeq(scala.collection.JavaConverters.asScalaIteratorConverter(x._2.listIterator()).asScala.toSeq))
                })


                val rdd = sc.makeRDD(List("xiaoxiaomo", "blog"))
                rdd.saveAsTextFile("/hbase/tmp/test"+i)




                val dataFrame = sql.createDataFrame(rows, schema)
                dataFrame.registerTempTable(value._1)

                val result = sql.sql("select * from "+value._1)

                result.write.save("/hbase/tmp/rowRDD"+i)

                result.rdd.collect().foreach(println)

            })

//
            i=i+1
        })











//        messages.foreachRDD(rdd => {rdd.foreachPartition(partitionRecords => {
//            partitionRecords.foreach(json => {
//
//                logger.error("json._2 ",json._2 )
//                val jsonObj: JSONObject = JSON.parseObject(json._2)
//
//                val qualifiers: JSONArray = jsonObj.getJSONArray("q")
//                val values: JSONArray = jsonObj.getJSONArray("v")
//
//                // 解析数据
//                rows.add(Row.fromSeq(scala.collection.JavaConverters.asScalaIteratorConverter(values.listIterator()).asScala.toSeq))
//
//                // 遍历解析schema
//                if( !q.containsKey(offsetRange.topic) ){
//                    val iterator = qualifiers.iterator()
//                    val schema = new StructType()
//                    while ( iterator.hasNext ){
//                        logger.error("iterator.hasNext")
//                        schema.add(StructField(iterator.next().toString, IntegerType, true))
//                        logger.error("schema",schema)
//                    }
//                    q.put(offsetRange.topic,schema)
//                }
//            })
//
//
//        })
        /*messages.foreachRDD(rdd => {




            // 默认五分钟执行一次
            var q = new util.HashMap[String,StructType]()
            var v = new util.HashMap[String,util.ArrayList[Row]]()
            val offsetsList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            //data 处理
            logger.error(s"data 等待处理。。。。。。")
            rdd.foreachPartition(partitionRecords => {
                // Topic每一个partition的信息
                // TaskContext 上下文
                val offsetRange: OffsetRange = offsetsList(TaskContext.get.partitionId)

                logger.error(s"${offsetRange.topic} ${offsetRange.partition} ${offsetRange.fromOffset} ${offsetRange.untilOffset}")

                //TopicAndPartition 主构造参数第一个是topic，第二个是Kafka partition id
                val topicAndPartition = TopicAndPartition(offsetRange.topic, offsetRange.partition)
//                val either = kc.setConsumerOffsets(groupName, Map((topicAndPartition, offsetRange.untilOffset))) //是
//                if (either.isLeft) {
//                    logger.info(s"Error updating the offset to Kafka cluster: ${either.left.get}")
//                }

                /** 解析PartitionRecords数据 */
                logger.error("解析PartitionRecords数据 start")
                if (offsetRange.topic != null) {

                    logger.error("解析PartitionRecords数据中。。。。")
                    logger.error("offsetRange.topic ",offsetRange.topic )
                    try {

                        /** 遍历Json数组 */
                        val rows = new util.ArrayList[Row]()

                        partitionRecords.foreach(json => {

                            logger.error("json._2 ",json._2 )
                            val jsonObj: JSONObject = JSON.parseObject(json._2)

                            val qualifiers: JSONArray = jsonObj.getJSONArray("q")
                            val values: JSONArray = jsonObj.getJSONArray("v")

                            // 解析数据
                            rows.add(Row.fromSeq(scala.collection.JavaConverters.asScalaIteratorConverter(values.listIterator()).asScala.toSeq))

                            // 遍历解析schema
                            if( !q.containsKey(offsetRange.topic) ){
                                val iterator = qualifiers.iterator()
                                val schema = new StructType()
                                while ( iterator.hasNext ){
                                    logger.error("iterator.hasNext")
                                    schema.add(StructField(iterator.next().toString, IntegerType, true))
                                    logger.error("schema",schema)
                                }
                                q.put(offsetRange.topic,schema)
                            }
                        })


                        if (v.containsKey(offsetRange.topic)){
                            val old = v.get(offsetRange.topic)
                            logger.error("old",old)
                            rows.addAll(old)
                        }
                        v.put(offsetRange.topic,rows)

                        val either = kc.setConsumerOffsets(groupName, Map((topicAndPartition, offsetRange.untilOffset))) //是
                        if (either.isLeft) {
                            logger.info(s"Error updating the offset to Kafka cluster: ${either.left.get}")
                        }
                    } catch {
                        case e: Exception =>
                            e.printStackTrace()
                            logger.error(s"解析数据 异常 ", e)
                            false
                    }
                }
            })

            val qualifier = q.keySet().iterator()
            logger.error(q.size()+"")
            logger.error(v.size()+"")
            while ( qualifier.hasNext ){
                val dataFrame = sql.createDataFrame(v.get(qualifier.next()),q.get(qualifier.next()))
                //    println( rDD )
                dataFrame.printSchema() //表结构
                dataFrame.show()
                dataFrame.registerTempTable( qualifier.next() )
            }
        })*/

        ssc.start()
        ssc.awaitTermination()
    }



    def jsonToRow( x : JSONArray ) : Seq[AnyRef] = {
        scala.collection.JavaConverters.asScalaIteratorConverter(x.listIterator()).asScala.toSeq
    }
}
