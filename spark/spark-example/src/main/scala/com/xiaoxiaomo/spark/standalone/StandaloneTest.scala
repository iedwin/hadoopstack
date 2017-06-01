package com.xiaoxiaomo.spark.standalone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


/**
  * standalone 任务提交
  * /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.standalone.StandaloneTest /home/txd/spark_scala-1.0-SNAPSHOT.jar spark://fetch-loadTest-22:7077 /opt/cloudera/parcels/CDH/lib/spark/ /data/test.txt /data/tmp
 */
object StandaloneTest {
  def main(args: Array[String]) {
    
    if(args.length != 4){
      println("Usage: <MasterURL> <SparkHome> <inputPath> <outputPath>")
      System.exit(1)
    }
	  //1. 构造配置对象
    val conf = new SparkConf
    conf.setAppName("standalone test") //job名称
    conf.setMaster(args(0))  //Spark URL
    conf.setSparkHome(args(1)) //spark安装目录
    conf.set("spark.executor.memory", "2g")  //配置自定义参数
    
    //2. 构造spark上下文对象
    val sc = new SparkContext(conf)
    
    //3. 加载数据
    val file = sc.textFile(args(2))
    
    //4. 计算逻辑
    val data = file.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    
    data.cache
    
    //5. 保存数据
    data.repartition(1).saveAsTextFile(args(3))
    
  }
}