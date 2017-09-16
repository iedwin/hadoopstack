package com.xiaoxiaomo.spark.rdd.r6

import com.xiaoxiaomo.spark.rdd.r5.ScoreDetail
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  *
  * reduceByKey 按照key进行Reduce
  *
  *
  * Created by TangXD on 2017/9/16.
  */
object reduceByKey {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("subtract rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        val RDD = sc.parallelize(List((1,2),(3,4),(3,6)))

        // 就是遇到相同的key时，就执行下面的函数，返回的key还是原来的key，value就为执行函数后的结果
        // 下面的x,y可以理解成两个相同key的两个value
        val reduceRDD = RDD.reduceByKey( (x,y) => x+y)
        reduceRDD.foreach(
            x => println(x)
        )

        // 单词统计
        val rdd = sc.textFile("spark/spark-example/src/main/resources/test/rdd_filter_1")
        rdd.flatMap( x=>x.split("\\s+")).map( x=>(x,1) )
                .reduceByKey( (x,y)=>x+y )
                .foreach(x=>println(x))


    }

}
