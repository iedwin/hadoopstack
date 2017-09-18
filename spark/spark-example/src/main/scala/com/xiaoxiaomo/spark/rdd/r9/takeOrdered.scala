package com.xiaoxiaomo.spark.rdd.r9

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * rdd.takeOrdered(n)
  *
  * 对RDD元素进行升序排序,取出前n个元素并返回，也可以自定义比较器，类似于top的相反的方法
  *
  * Created by TangXD on 2017/9/18.
  */
object takeOrdered {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.makeRDD(Array((1,2),(2,3),(2,6)))
        val rdd2 = sc.parallelize(List(1,2,3,3))


//        (1,2)
//        (2,3)
//        1
//        2
        rdd1.takeOrdered(2).foreach(x=>println(x))
        rdd2.takeOrdered(2).foreach(x=>println(x))


    }
}
