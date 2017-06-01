package com.xiaoxiaomo.spark.function

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
/**
 * RDD的转换过程
 */
object Function5 {
  def main(args: Array[String]) {
	  //创建一个本地调试配置对象
     val conf = new SparkConf
     conf.setAppName("test function")
     conf.setMaster("local")
     
     //上下文对象
     val sc = new SparkContext(conf)
     val HadoopRDD = sc.textFile("c://a.txt")
     val FlatMappedRDD = HadoopRDD.flatMap(_.split(" "))
     val MappedRDD = FlatMappedRDD.map((_,1))
     val ShuffledRDD = MappedRDD.reduceByKey(_ + _)
     //使用上下文对象设置checkpoint目录
     sc.setCheckpointDir("hdfs://192.168.1.254:8020/user/gs/checkpoint")
     println(ShuffledRDD)
     //对需要做checkpoint的RDD调用checkpoint方法
     ShuffledRDD.checkpoint
     
//     val rdd = sc.textFile("c://a.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
     
  }
}