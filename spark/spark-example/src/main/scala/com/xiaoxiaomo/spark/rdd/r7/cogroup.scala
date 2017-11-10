package com.xiaoxiaomo.spark.rdd.r7

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jason on 17-9-17.
  */
object cogroup {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("subtract rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        val scoreDetail = sc.parallelize(List(("xiaoming",95),("xiaoming",90),("lihua",95),("lihua",98),("xiaofeng",97)))
        val scoreDetai2 = sc.parallelize(List(("xiaoming",65),("lihua",63),("lihua",62),("xiaofeng",67)))
        val scoreDetai3 = sc.parallelize(List(("xiaoming",25),("xiaoming",15),("lihua",35),("lihua",28),("xiaofeng",36)))
        scoreDetail.cogroup(scoreDetai2,scoreDetai3)


    }

}
