package com.xiaoxiaomo.spark.rdd.r3

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * distinct 用于去重
  *
  * Created by TangXD on 2017/9/16.
  */
object distinct {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setAppName("parallelize rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val RDD1 = sc.parallelize(List("aa","aa","bb","cc","dd"))

        println( RDD1.collect().toList )

        println( RDD1.distinct().collect().toList )


    }

}
