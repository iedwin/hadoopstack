package com.xiaoxiaomo.spark.rdd.r8

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * rightOuterJoin
  *
  * 对两个 RDD 进行连接操作，类似于sql中的右外连接，存在的话，value用的Some, 不存在用的None,
  *
  * Created by TangXD on 2017/9/18.
  */
object rightOuterJoin {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.makeRDD(Array((1,2),(2,3),(2,6)))
        val rdd2 = sc.makeRDD(Array((5,2),(2,9)))

        // List((5,(None,2)), (2,(Some(3),9)), (2,(Some(6),9)))
        val rdd = rdd1.rightOuterJoin(rdd2)

        println(rdd.collect().toList)

    }
}
