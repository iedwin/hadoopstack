package com.xiaoxiaomo.spark.rdd.r3

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * subtract
  *
  *
  * Created by TangXD on 2017/9/16.
  */
object subtract {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("subtract rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        // RDD1.subtract(RDD2)
        // 返回在RDD1中出现，但是不在RDD2中出现的元素，不去重
        val RDD1 = sc.parallelize(List("aa","aa","bb","cc","dd"))
        val RDD2 = sc.parallelize(List("aa","dd","ff"))


        println( RDD1.subtract(RDD2).collect().toList )

    }

}
