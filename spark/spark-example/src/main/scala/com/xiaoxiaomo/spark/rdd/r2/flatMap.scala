package com.xiaoxiaomo.spark.rdd.r2

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * flatMap
  *
  * 有时候，我们希望对某个元素生成多个元素，实现该功能的操作叫作 flatMap()
  *
  * 将函数应用于RDD中的每个元素，将返回的迭代器的所有内容构成的新的RDD。通常用来切分单词
  *
  * Created by TangXD on 2017/9/16.
  */
object flatMap {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setAppName("parallelize rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd = sc.textFile("spark/spark-example/src/main/resources/test/rdd_filter_1") //行加载


        // flatMap的函数应用于每一个元素，对于每一个元素返回的是多个元素组成的迭代器
        val newRDD = rdd.flatMap(x => x.split("\\s+"))

        println(newRDD)
        newRDD.collect().foreach(println(_))

        println(newRDD.first())
    }
}
