package com.xiaoxiaomo.spark.rdd.r2

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * map
  *
  * 函数应用于RDD的每一个元素，将返回值构造为新的RDD
  *
  *
  * Created by TangXD on 2017/9/16.
  */
object map {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setAppName("parallelize rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd = sc.textFile("spark/spark-example/src/main/resources/test/rdd_filter_1") //行加载


        // //用map，对于每一行数据，按照空格分割成一个一个数组，然后返回的是一对一的关系
        val newRDD = rdd.map(x => x.split("\\s+"))

        println(newRDD)
        newRDD.collect().foreach(println(_))

        println(newRDD.first())
    }
}
