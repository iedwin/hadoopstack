package com.xiaoxiaomo.spark.streaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * StreamingStatusOpera 持续处理
  *
  * 执行 ：
  *   /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.streaming.StreamingStatusOpera original-spark_scala-1.0-SNAPSHOT.jar 3 10.141.5.26 9999 local[2]
  *   /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.streaming.StreamingStatusOpera original-spark_scala-1.0-SNAPSHOT.jar 3 10.141.5.26 9999 spark://fetch-loadTest-20:7077
  * Created by xiaoxiaomo on 2016/11/17.
  */
object StreamingStatusOpera {

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf()
    conf.setMaster(args(3))
    conf.setAppName("StreamingStatusOpera")
    val ssc: StreamingContext = new StreamingContext(conf , Seconds(args(0).toInt))

    //设置检查点目录
    ssc.checkpoint("/tmp")


    //3. 接收数据
    //参数一：发送socket消息的主机  参数二：发送socket消息的端口  参数三：存储级别
    val data: ReceiverInputDStream[String] = ssc.socketTextStream(args(1),args(2).toInt,StorageLevel.MEMORY_AND_DISK)

    //4. 业务逻辑
    val rs: DStream[(String, Int)] = data.flatMap( _.split(" ") ).map((_,1)).reduceByKey(_ + _)

    rs.updateStateByKey(wordCountFunc)
    rs.print


    ssc.start
    ssc.awaitTermination

  }

  //Seq[Int]一种scala集合，可以存储重复数据，可以快速插入和删除数据数据（有序）
  //Option[Int]也是一种集合，如果有值，返回Some[A]，如果没值，返回NONE
  def wordCountFunc  = ( value:Seq[Int] , status:Option[Int] ) => {
    //累加当前状态
    val data = value.foldLeft(0)(_ + _)
    //取出过去状态   括号中是初始值  第一次为0
    val last = status.getOrElse(0)
    //返回
    Some(data + last)
  }


}
