package com.xiaoxiaomo.spark.rdd.r7

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * Created by jason on 17-9-17.
  */
object groupByKey {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("subtract rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        val RDD = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))

        //
        val reduceRDD = RDD.groupByKey()
        reduceRDD.foreach(
            x => println(x)
        )


    }
}
