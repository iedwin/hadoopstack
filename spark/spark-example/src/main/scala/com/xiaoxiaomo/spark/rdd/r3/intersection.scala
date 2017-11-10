package com.xiaoxiaomo.spark.rdd.r3

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 返回两个RDD的交集，并且去重
  *
  * Created by TangXD on 2017/9/16.
  */
object intersection {


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("parallelize rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val RDD1 = sc.parallelize(List("aa","aa","bb","cc","dd"))
        val RDD2 = sc.parallelize(List("aa","dd","ff"))


        println( RDD1.intersection(RDD2).collect().toList )

    }

}
