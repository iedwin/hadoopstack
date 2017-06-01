package com.xiaoxiaomo.spark.function

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Function4 {
  def main(args: Array[String]) {
	  //创建一个本地调试配置对象
     val conf = new SparkConf
     conf.setAppName("test function")
     conf.setMaster("local")
     
     //上下文对象
     val sc = new SparkContext(conf)
     val rdd = sc.makeRDD(Array(1,2,3,4,5,5))
//     val rdd1 = rdd.repartition(1)
     val rdd1 = rdd.coalesce(1, false)
//     rdd1.foreach(println _) 
//     println(rdd1.partitions.size)
     println(rdd.countByValue)
  }
}