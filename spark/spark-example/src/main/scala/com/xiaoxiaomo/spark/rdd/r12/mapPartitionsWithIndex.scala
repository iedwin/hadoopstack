package com.xiaoxiaomo.spark.rdd.r12

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by TangXD on 2017/9/18.
  */
object mapPartitionsWithIndex {


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))


//        var mapPartIndexRDDs = rdd1.mapPartitionsWithIndex(mapPartIndexFunc)
//
//        mapPartIndexRDDs.foreach(println( _))

    }

    def mapPartIndexFunc(i1:Int,iter: Iterator[Int]):Iterator[(Int,Int)]={
        var res = List[(Int,Int)]()
        while(iter.hasNext){
            var next = iter.next()
            res=res.::(i1,next)
        }
        res.iterator
    }

}
