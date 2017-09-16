package com.xiaoxiaomo.spark.rdd.r3

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * union 合并l两个RDD
  *
  *
  * Created by TangXD on 2017/9/16.
  */
object union {


    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setAppName("parallelize rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val RDD1 = sc.parallelize(List("aa","aa","bb","cc","dd"))
        val RDD2 = sc.parallelize(List("aa","dd","ff"))


        println( RDD1.union(RDD2).collect().toList )

    }

}
