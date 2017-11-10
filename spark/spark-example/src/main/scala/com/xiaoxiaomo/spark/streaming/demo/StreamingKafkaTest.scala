package com.xiaoxiaomo.spark.streaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * StreamingStatusOpera 持续处理
  *
  * 执行 ：
  * /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.streaming.StreamingKafka original-spark_scala-1.0-SNAPSHOT.jar 5 local[2]
  * Created by xiaoxiaomo on 2016/11/17.
  */
object StreamingKafkaTest {

    def main(args: Array[String]) {

        val conf: SparkConf = new SparkConf()
        conf.setMaster(args(1))
        conf.setAppName("StreamingKafka")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(args(0).toInt))

        //设置检查点目录
        ssc.checkpoint("/tmp")


        //3. 接收数据
        //参数一：发送socket消息的主机  参数二：发送socket消息的端口  参数三：存储级别
        val topicsSet = "CarrierFetch".split(",").toSet
        val kafkaParams = scala.collection.immutable.Map[String, String](
            "metadata.broker.list" -> "xiaoxiaomo:9092",
            "auto.offset.reset" -> "smallest",
            "group.id" -> "hbaseGroup")
//            val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream(ssc, kafkaParams, topicsSet)
////        val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
//
//
//        //4. 业务逻辑
//        stream.map(_._2) // 取出value
//                .flatMap(_.split(" ")) // 将字符串使用空格分隔
//                .map(r => (r, 1)) // 每个单词映射成一个pair
//                .updateStateByKey[Int](updateFunc) // 用当前batch的数据区更新已有的数据
//                .print() // 打印前10个数据

        ssc.start
        ssc.awaitTermination

    }

    //Seq[Int]一种scala集合，可以存储重复数据，可以快速插入和删除数据数据（有序）
    //Option[Int]也是一种集合，如果有值，返回Some[A]，如果没值，返回NONE
    def wordCountFunc = (value: Seq[Int], status: Option[Int]) => {
        //累加当前状态
        val data = value.foldLeft(0)(_ + _)
        //取出过去状态   括号中是初始值  第一次为0
        val last = status.getOrElse(0)
        //返回
        Some(data + last)
    }


    val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
        val curr = currentValues.sum
        val pre = preValue.getOrElse(0)
        Some(curr + pre)
    }


}
