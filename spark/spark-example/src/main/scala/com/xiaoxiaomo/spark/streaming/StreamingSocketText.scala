package com.xiaoxiaomo.spark.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

/**
  *
  * nc -lk 9999
  *
  * /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.streaming.StreamingSocketText original-spark_scala-1.0-SNAPSHOT.jar localhost 9999
  * Created by xiaoxiaomo on 2017/4/24.
  */
object StreamingSocketText {

  def main(args: Array[String]) {

    // Create a local StreamingContext with two working thread and batch interval of 1 second
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()


    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }


}
