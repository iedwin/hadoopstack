package com.xiaoxiaomo.spark.rdd.r11

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
/**
  * Created by TangXD on 2017/9/18.
  */
object saveAsNewAPIHadoopFile {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))

        rdd1.saveAsNewAPIHadoopFile("file:///tmp/5",classOf[Text],classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]])


    }


}
