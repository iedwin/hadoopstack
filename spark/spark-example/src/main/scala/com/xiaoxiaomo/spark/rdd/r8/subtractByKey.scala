package com.xiaoxiaomo.spark.rdd.r8

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jason on 17-9-17.
  */
object subtractByKey {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("subtract rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.makeRDD(Array((1,2),(2,3),(2,6)))
        val rdd2 = sc.makeRDD(Array((5,2),(2,9)))

        rdd1.subtractByKey(rdd2)

    }

}
