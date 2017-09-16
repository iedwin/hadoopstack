package com.xiaoxiaomo.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * blog : http://blog.csdn.net/t1dmzks/article/details/70189509
  *
  * Created by TangXD on 2017/9/16.
  */
object parallelize {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setAppName("parallelize rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        val rdd = sc.parallelize(List("xiaoxiaomo", "blog"))

        println(rdd)

    }

}
