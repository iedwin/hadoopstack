package com.xiaoxiaomo.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实时流处理 DStream一组时间序列上连续的RDD 以HDFS作为数据源
  * 默认存储级别为<内存+磁盘>
  *   可以使用persist()函数对其进行序列化(优选KryoSerializer)
  *
  * 执行 - 监控/tmp/test目录：
  *   /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.streaming.StreamingDealWithHDFS original-spark_scala-1.0-SNAPSHOT.jar 3 /tmp/test
  *
  * Created by xiaoxiaomo on 2016/6/17.
  */
object StreamingDealWithHDFS {

  def main(args: Array[String]) {

    //1. 实例化配置文件
    val conf: SparkConf = new SparkConf()

    //2. steaming上下文
    val ssc: StreamingContext = new StreamingContext(conf , Seconds(args(0).toInt))


    //3. 接收数据
    val data: DStream[String] = ssc.textFileStream( args(1) ) //args(1) 为目录

    //4. 业务逻辑
    val rs: DStream[(String, Int)] = data.flatMap( _.split(" ") ).map((_,1)).reduceByKey(_ + _)

    //打印结果集 - 注意：没有累加
    rs.print


    //5. 启动任务  需要使用上下文对象启动
    ssc.start
    //等待任务完成，让它持续处理
    ssc.awaitTermination

  }

}
