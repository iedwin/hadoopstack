package com.xiaoxiaomo.spark.rdd.r11

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put

/**
  * Created by TangXD on 2017/9/18.
  */
object saveAsNewAPIHadoopDataset {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("rdd")
        conf.setMaster("local")
        val sc = new SparkContext(conf)


        val rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))

        sc.hadoopConfiguration.set("hbase.zookeeper.quorum ","zkNode1,zkNode2,zkNode3")
        sc.hadoopConfiguration.set("zookeeper.znode.parent","/hbase")
        sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,"lxw1234")
        var job = new Job(sc.hadoopConfiguration)
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Result])
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

        rdd1.map(
            x => {
                var put = new Put(Bytes.toBytes(x._1))
                put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(x._2))
                (new ImmutableBytesWritable,put)
            }
        ).saveAsNewAPIHadoopDataset(job.getConfiguration)

        sc.stop()
    }

}
