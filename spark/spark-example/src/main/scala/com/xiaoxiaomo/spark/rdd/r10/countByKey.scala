package com.xiaoxiaomo.spark.rdd.r10

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * countByKey 通过key统计元素个数
  *
  * Created by TangXD on 2017/9/18.
  */
object countByKey {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.parallelize(Array((1, 2),(2,4),(2,5), (3, 4),(3,5), (3, 6)))

        //        (1,1)
        //        (3,3)
        //        (2,2)
        rdd1.countByKey.foreach(x=>println(x))


    }
}
