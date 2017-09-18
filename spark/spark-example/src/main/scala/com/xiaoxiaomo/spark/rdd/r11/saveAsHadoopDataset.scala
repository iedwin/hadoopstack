package com.xiaoxiaomo.spark.rdd.r11

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import SparkContext._
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

/**
  *
  *
  * aveAsHadoopDataset用于将RDD保存到除了HDFS的其他存储中，比如HBase。
  *
  *
  * 在JobConf中，通常需要关注或者设置五个参数：
  *
  * 文件的保存路径、key值的class类型、value值的class类型、RDD的输出格式(OutputFormat)、以及压缩相关的参数。
  *
  * Created by TangXD on 2017/9/18.
  */
object saveAsHadoopDataset {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))

        // 使用saveAsHadoopDataset将RDD保存到HDFS中
        var jobConf = new JobConf()
        jobConf.setOutputFormat(classOf[TextOutputFormat[Text,IntWritable]])
        jobConf.setOutputKeyClass(classOf[Text])
        jobConf.setOutputValueClass(classOf[IntWritable])
        jobConf.set("mapred.output.dir","/tmp/4/")
        rdd1.saveAsHadoopDataset(jobConf)




        // 保存到hbase
        jobConf.set("hbase.zookeeper.quorum","xiaoxiaomo")
        jobConf.set("zookeeper.znode.parent","/hbase")
        jobConf.set(TableOutputFormat.OUTPUT_TABLE,"xiaoxiaomo")
        jobConf.setOutputFormat(classOf[TableOutputFormat])


        rdd1.map(x => {
            var put = new Put(Bytes.toBytes(x._1))
            put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(x._2))
            (new ImmutableBytesWritable,put)
        }).saveAsHadoopDataset(jobConf)

    }
}
