package com.xiaoxiaomo.spark.rdd.r5

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  *
  * https://www.edureka.co/blog/apache-spark-combinebykey-explained
  *
  * 聚合数据一般在集中式数据比较方便，如果涉及到分布式的数据集，该如何去实现呢。这里介绍一下combineByKey, 这个是各种聚集操作的鼻祖，应该要好好了解一下,参考scala API
  *
  * Created by TangXD on 2017/9/16.
  */
object combineByKey {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("subtract rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        val scores = List(
            ScoreDetail("xiaoming", "Math", 98),
            ScoreDetail("xiaoming", "English", 88),
            ScoreDetail("wangwu", "Math", 75),
            ScoreDetail("wangwu", "English", 78),
            ScoreDetail("lihua", "Math", 90),
            ScoreDetail("lihua", "English", 80),
            ScoreDetail("zhangsan", "Math", 91),
            ScoreDetail("zhangsan", "English", 80))


        val scoresWithKey = for { i <- scores } yield (i.studentName, i)

        //创建RDD, 并且指定三个分区
        val scoresWithKeyRDD = sc.parallelize(scoresWithKey).partitionBy(new HashPartitioner(3)).cache

        println(">>>> Elements in each partition")

        scoresWithKeyRDD.foreachPartition(partition => println(partition.length))

        // explore each partition...
        println(">>>> Exploring partitions' data...")

        scoresWithKeyRDD.foreachPartition(
            partition => partition.foreach(
                item => println(item._2)))




        val avgScoresRDD = scoresWithKeyRDD.combineByKey(
            (x: ScoreDetail) => (x.score, 1) /*createCombiner*/,
            (acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1) /*mergeValue*/,
            (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) /*mergeCombiners*/
            // calculate the average
        ).map( { case(key, value) => (key, value._1/value._2) })

        avgScoresRDD.collect.foreach(println)


    }
}
