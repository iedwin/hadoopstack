package com.xiaoxiaomo.spark.rdd.r9

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * rdd.reduce(func)
  *
  * 并行整合RDD中所有数据， 类似于是scala中集合的reduce
  *
  *
  * Created by TangXD on 2017/9/18.
  */
object reduce {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd2 = sc.parallelize(List(1,2,3,3))

        println(rdd2.reduce((x,y) => x+y))


    }

}
