package com.xiaoxiaomo.spark.rdd.r10

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 将pair类型(键值对类型)的RDD转换成map, 如果有相同的key只取后面的一个
  *
  *
  * Created by TangXD on 2017/9/18.
  */
object collectAsMap {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.parallelize(Array((1, 2),(2,4),(2,5), (3, 4),(3,5), (3, 6)))

//        (2,5)
//        (1,2)
//        (3,6)
        rdd1.collectAsMap.foreach(x=>println(x))


    }
}
