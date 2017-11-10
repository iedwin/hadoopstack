package com.xiaoxiaomo.spark.rdd.r6

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * foldByKey
  *
  * 简单理解比ReduceByKey多了一个初始值，该值作用于每一个Value
  *
  * Created by TangXD on 2017/9/16.
  */
object foldByKey {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("subtract rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        val RDD = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))

        //
        val reduceRDD = RDD.foldByKey(0)((x,y) => x+y)
        reduceRDD.foreach(
            x => println(x)
        )



        println(RDD.foldByKey(2)(_+_).collect.toList)
        println(RDD.foldByKey(0)(_*_).collect.toList)
        println(RDD.foldByKey(2)(_*_).collect.toList)


    }

}
