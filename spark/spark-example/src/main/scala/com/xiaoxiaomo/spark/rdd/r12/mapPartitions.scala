package com.xiaoxiaomo.spark.rdd.r12

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{JobConf, TextOutputFormat}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by TangXD on 2017/9/18.
  */
object mapPartitions {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))


//        val mapPartRDD = rdd1.mapPartitions(mapPartFunc)
//        mapPartRDD.foreach(maps=>println(maps))

    }

    def mapPartFunc(iter: Iterator[Int]):Iterator[(Int,Int)]={
        var res = List[(Int,Int)]()
        while (iter.hasNext){
            val cur = iter.next
            res=res.::(cur,cur*cur)
        }
        res.iterator
    }
}
