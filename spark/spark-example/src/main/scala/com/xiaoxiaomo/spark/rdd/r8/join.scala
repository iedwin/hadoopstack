package com.xiaoxiaomo.spark.rdd.r8

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * rdd.join(otherRDD) 针对(k,v)类型的RDD
  *
  * 把rdd,otherRDD中的相同的key给连接起来，类似于sql中的join操作
  *
  * Created by TangXD on 2017/9/18.
  */
object join {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("subtract rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.makeRDD(Array((1,2),(2,3),(2,6)))
        val rdd2 = sc.makeRDD(Array((5,2),(2,9)))

        val rdd = rdd1.join(rdd2)

        // List((2,(3,9)), (2,(6,9)))
        println(rdd.collect().toList)

    }

}
