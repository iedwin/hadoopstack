package com.xiaoxiaomo.spark.rdd.r9

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * 各元素在 RDD 中出现的次数 返回{(key1,次数),(key2,次数),…(keyn,次数)}
  *
  *
  * Created by TangXD on 2017/9/18.
  */
object countByValue {


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.makeRDD(Array((1,2),(2,3),(2,6)))
        val rdd2 = sc.parallelize(List(1,2,3,3))


//        ((2,3),1)
//        ((2,6),1)
//        ((1,2),1)
//        (1,1)
//        (3,2)
//        (2,1)
        rdd1.countByValue().foreach(x =>println(x))
        rdd2.countByValue().foreach(x =>println(x))


    }

}
