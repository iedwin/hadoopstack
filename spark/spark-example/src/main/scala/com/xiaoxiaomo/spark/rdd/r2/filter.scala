package com.xiaoxiaomo.spark.rdd.r2

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * filter
  *
  * 理解：一个过滤器函数
  * 调用该函数，函数应用于每个元素，返回一个由拦截过滤后的元素组成的新RDD
  *
  *
  * Created by TangXD on 2017/9/16.
  */
object filter {


    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setAppName("parallelize rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd = sc.textFile("spark/spark-example/src/main/resources/test/rdd_filter_1") //行加载
//        rdd.collect().foreach(println(_))


        // 通过filter 过滤含有xiaoxiaomo的字段
        val newRDD = rdd.filter( x => x.contains("xiaoxiaomo"))

        newRDD.collect().foreach(println(_))

    }

}
