package com.xiaoxiaomo.spark.rdd.r4

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * scala是没有mapToPair函数的，scala版本只需要map就可以了
  *
  * Created by TangXD on 2017/9/16.
  */
object mapToPair {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("subtract rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        val rdd = sc.textFile("spark/spark-example/src/main/resources/test/rdd_filter_1")
        val pairs = rdd.map(x => (x.split("\\s+")(0), 1))


        // List((aa,1), (ff,1), (ee,1), (ee,1))
        println( pairs.collect().toList )

    }
}
