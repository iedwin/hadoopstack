package com.xiaoxiaomo.spark.rdd.r9

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 按照降序的或者指定的排序规则，返回前n个元素
  *
  * Created by TangXD on 2017/9/18.
  */
object top {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.makeRDD(Array((1,2),(2,3),(2,6)))
        val rdd2 = sc.parallelize(List(1,2,3,3),2)

//        (2,6)
//        (2,3)
//        2
//        3
//        3
        rdd1.top(2).foreach(x=>println(x))
        println(rdd2.getNumPartitions)
        rdd2.top(2).foreach(x=>println(x))


    }
}
