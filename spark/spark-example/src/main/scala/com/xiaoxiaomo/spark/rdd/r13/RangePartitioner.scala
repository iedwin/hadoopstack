package com.xiaoxiaomo.spark.rdd.r13

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * RangePartitioner
  *
  *
  * Created by TangXD on 2017/9/18.
  */
object RangePartitioner {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val pairRdd = sc.parallelize(List((1,1), (1,2), (2,3), (2,4), (3,5), (3,6),(4,7), (4,8),(5,9), (5,10)))
        //未分区的输出
        println(pairRdd.partitioner)
        println("=========================")
        //        val partitioned = pairRdd.RangePartitioner(new spark.HashPartitioner(3))
        //分区后的输出
        println(pairRdd.partitioner)


    }

}
