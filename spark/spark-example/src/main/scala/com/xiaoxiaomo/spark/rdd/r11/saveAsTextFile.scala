package com.xiaoxiaomo.spark.rdd.r11

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * saveAsTextFile 用于将RDD以文本文件的格式存储到文件系统中。
  *
  * Created by TangXD on 2017/9/18.
  */
object saveAsTextFile {


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.parallelize(Array((1, 2),(2,4),(2,5), (3, 4),(3,5), (3, 6)))

        rdd1.saveAsTextFile("file:///tmp")


    }

}
