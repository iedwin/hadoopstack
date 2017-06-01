package com.xiaoxiaomo.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * StreamingWCByWindow2
  *
  * 执行：
  *   /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.streaming.StreamingWCByWindow2 original-spark_scala-1.0-SNAPSHOT.jar 3 192.168.1.158 9091 15 15
  *
  * Created by xiaoxiaomo on 2016/8/17.
  */
object StreamingWCByWindow2 {

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf()
    val ssc: StreamingContext = new StreamingContext(conf , Seconds(args(0).toInt))

    ssc.checkpoint("/tmp/test")

    //3. 接收数据
    //参数一：发送socket消息的主机  参数二：发送socket消息的端口  参数三：存储级别
    val data: ReceiverInputDStream[String] = ssc.socketTextStream(args(1),args(2).toInt,StorageLevel.MEMORY_AND_DISK)

    //4. 业务逻辑
    //参数一累加函数  ，参数二表示减去最初状态的函数 ， 参数三数据时间间隔 ， 参数四处理时间间隔（应该大于等于参数三）
    val rs = data.map((_,1)).reduceByKeyAndWindow(_ + _ , _ - _, Seconds(args(3).toInt), Seconds(args(4).toInt))
    //   rs.checkpoint(interval)

    rs.updateStateByKey(StreamingStatusOpera.wordCountFunc)
    rs.print

    ssc.start
    ssc.awaitTermination

  }

}
