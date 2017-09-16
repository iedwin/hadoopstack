package com.xiaoxiaomo.spark.rdd.r6

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  *
  * SortByKey
  *
  * 用于对RDD按照key进行排序，第一个参数可以设置true或者false，默认是true
  *
  *
  * Created by TangXD on 2017/9/16.
  */
object SortByKey {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("subtract rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        val RDD = sc.parallelize(Array((3, 4), (1, 2), (4, 4), (2, 5), (6, 5), (5, 6)))
        RDD.sortByKey().foreach(x=>print(x))

        RDD.sortByKey(false).foreach(x=>print(x)) // 降序



    }

}
