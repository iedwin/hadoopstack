package com.xiaoxiaomo.spark.onyarn

import org.apache.spark.SparkContext

/**
  *
  * 提交任务命令
  * /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.onyarn.OnYarnTest --master yarn-client /home/txd/spark_scala-1.0-SNAPSHOT.jar /data/test.txt /data/tmp
  *
  * 代码开发流程
  * 1. 构造sc对象  val sc = new SparkContext
  * 2. 加载数据
  * 3. 业务逻辑
  * 4. 输出
  *
  * yarn-client 和 yarn-cluster 区别。
  * 1. yarn-client日志打印比yarn-cluster更加详细。
  * 2. yarn-client会立即校验程序，而yarn-cluster在程序申请到资源之后执行在校验。
  * 3. spark Driver的位置不一样，yarn-client和提交client在一台机器，yarn-cluster随机在机器中AM所在机器启动。
  */
object OnYarnTest {
  def main(args: Array[String]) {
    
    if(args.length != 2){
      println("Usage: <inputPath> <outputPath>")
      System.exit(1)
    }
    
    //1. 构造spark上下文对象
    val sc = new SparkContext
    
    //2. 加载数据
    val file = sc.textFile(args(0))
    
    //3. 计算逻辑
    val data = file.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    
    data.cache
    
    //4. 保存数据
    data.repartition(1).saveAsTextFile(args(1))
    
  }
}