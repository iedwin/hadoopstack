package com.xiaoxiaomo.spark.function

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xiaoxiaomo on 2016/6/17.
 */
object T1_ {

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf()
    conf.setAppName("t1_")
    conf.setMaster("local")

    val context: SparkContext = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = context.textFile("/src/main/resources/a.txt").flatMap( _.split(" ") ).map((_ , 1)).reduceByKey(_ + _)

    rdd.foreach( println _ )


  }

}
