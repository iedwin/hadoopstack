package com.xiaoxiaomo.spark.rdd.r9

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * rdd.collect() 返回 RDD 中的所有元素
  *
  * Created by TangXD on 2017/9/18.
  */
object collect {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.makeRDD(Array((1,2),(2,3),(2,6)))
        val rdd2 = sc.parallelize(List(1,2,3,3))

        println(rdd1.collect().toList)
        println(rdd2.collect().toList)


    }
}
