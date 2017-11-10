package com.xiaoxiaomo.spark.rdd.r11

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * saveAsObjectFile用于将RDD中的元素序列化成对象，存储到文件中。
  *
  * Created by TangXD on 2017/9/18.
  */
object saveAsObjectFile {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.parallelize(Array((1, 2),(2,4),(2,5), (3, 4),(3,5), (3, 6)))

        rdd1.saveAsObjectFile("file:///tmp3")


    }

}
