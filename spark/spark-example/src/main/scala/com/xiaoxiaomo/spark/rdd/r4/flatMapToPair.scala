package com.xiaoxiaomo.spark.rdd.r4

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * flatMapToPair
  *
  *
  * Created by TangXD on 2017/9/16.
  */
object flatMapToPair {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("subtract rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        val rdd = sc.textFile("spark/spark-example/src/main/resources/test/rdd_filter_1")
        val flatRDD  = rdd.flatMap(x => (x.split("\\s+")))
        val pairs = flatRDD.map(x=>(x,1))

        // List((aa,1), (bb,1), (cc,1), (aa,1), (aa,1), (aa,1), (dd,1), (dd,1), (ee,1), (ee,1), (ee,1), (ee,1), (ff,1), (aa,1), (bb,1), (xiaoxiaomo,1), (ee,1), (kks,1), (ee,1), (zz,1), (xiaoxiaomo,1))
        println( pairs.collect().toList )

    }
}
