package com.xiaoxiaomo.spark.rdd.r3

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * RDD1.cartesian(RDD2) 返回RDD1和RDD2的笛卡儿积，这个开销非常大
  *
  *
  * Created by TangXD on 2017/9/16.
  */
object cartesian {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("subtract rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        // RDD1.subtract(RDD2)
        // 返回在RDD1中出现，但是不在RDD2中出现的元素，不去重
        val RDD1 = sc.parallelize(List("aa","aa","bb","cc","dd"))
        val RDD2 = sc.parallelize(List("aa","dd","ff"))


        // List((aa,aa), (aa,dd), (aa,ff), (aa,aa), (aa,dd), (aa,ff), (bb,aa), (bb,dd), (bb,ff), (cc,aa), (cc,dd), (cc,ff), (dd,aa), (dd,dd), (dd,ff))
        println( RDD1.cartesian(RDD2).collect().toList )

    }
}
