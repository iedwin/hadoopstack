package com.xiaoxiaomo.spark.rdd.r1

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * Created by TangXD on 2017/9/16.
  */
object textFile {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("parallelize rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd = sc.textFile("/test/rdd_testfile_1")

        println(rdd)



    }

}
