package com.xiaoxiaomo.spark.rdd.r9

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * rdd.fold(num)(func)
  *
  * 和 reduce() 一 样， 但是提供了初始值num
  *
  * 先要合这个初始值进行折叠, 注意，这里会按照每个分区进行fold，然后分区之间还会再次进行fold提供初始值
  *
  *
  * Created by TangXD on 2017/9/18.
  */
object fold {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        val rdd1 = sc.makeRDD(Array((1,2),(2,3),(2,6)))
        val rdd2 = sc.parallelize(List(1,2,3,3),2)

        // 15 =
        println(rdd2.getNumPartitions)
        println(rdd2.fold(2)((x,y)=>x+y))


    }
}
