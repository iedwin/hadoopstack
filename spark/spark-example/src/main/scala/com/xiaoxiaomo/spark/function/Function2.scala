package com.xiaoxiaomo.spark.function

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Function2 {
  def main(args: Array[String]) {
	  //创建一个本地调试配置对象
     val conf = new SparkConf
     conf.setAppName("test function")
     conf.setMaster("local")
     
     //上下文对象
     val sc = new SparkContext(conf)
     val rdd = sc.makeRDD(Array(1,2,3,4,5,6,7,8,9),2)
     val rs = rdd.aggregate(0)(math.max(_, _), _ + _)
     println(rs)
     println(rdd.partitions.size) 
     
  }
}