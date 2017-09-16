package com.xiaoxiaomo.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * Created by TangXD on 2017/9/16.
  */
object makeRDD {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("parallelize rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd = sc.makeRDD(List("xiaoxiaomo", "blog"))    //其实内部就是调用的parallelize

        println(rdd)
    }

}
