package com.xiaoxiaomo.spark.function

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Function3 {
  def main(args: Array[String]) {
	  //创建一个本地调试配置对象
     val conf = new SparkConf
     conf.setAppName("test function")
     conf.setMaster("local")
     
     //上下文对象
     val sc = new SparkContext(conf)
     val rdd1 = sc.makeRDD(Array(('A',1),('B',1)))
     val rdd2 = sc.makeRDD(Array(('C',1),('D',1),('A',3)))
     var rdd3 = rdd1.join(rdd2)
     rdd3.foreach(println _)
     println(rdd3.partitions.size) 
  }
}