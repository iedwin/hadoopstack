package com.xiaoxiaomo.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark SQL 读取Json File
  *
  * 运行方式：
  *   1. /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.sql.ReadJsonStr  spark_scala-1.0-SNAPSHOT.jar local
  *   2. /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.sql.ReadJsonStr  spark_scala-1.0-SNAPSHOT.jar yarn-client
  *
  * Created by xiaoxiaomo on 2016/6/18.
  *
  */
object ReadJsonStr {

  def main(args: Array[String]) {

    //1. 加载配置文件spark.driver.allowMultipleContexts
    val conf: SparkConf = new SparkConf()
    conf.setAppName( "Spark-SQL ReadJsonStr" )
    conf.set("spark.driver.allowMultipleContexts","true") //TODO

    var master = "yarn-client" //"local"
    if( args.length > 0 ){
      master = args{0}
    }
    conf.setMaster(master)

    //2. 实例化上下文
    val sc: SparkContext = new SparkContext(conf)


    //2.1 实例化SQL上下文
    val context: SQLContext = new SQLContext(sc)
    //    val hiveContext: HiveContext = new HiveContext( sc ) //推荐使用


    //3.读取数据源
    val list = List("""{"name":"zhangsan","age":18}""","""{"name":"lisi","age":25,"saraly":19000}""","""{"name":"wangwu","age":19,"saraly":16000}""")
    val strRdd: RDD[String] = sc.parallelize( list )

    val json: DataFrame = context.read.json(strRdd)

    //    println( rDD )
    json.printSchema() //表结构
    json.show()


    println( "================================================ > " )

    //表操作
    //1. 注册表
    json.registerTempTable( "t_xiaoxiaomo" )

    //2. 创建sql
    val rs: DataFrame = context.sql("select * from t_xiaoxiaomo ")
    //    rs.foreach( println _ ) //遍历result数据

    //遍历
    rs.map( row => (row(1) , row(0) ,row(2) ) ).foreach(println)

    //遍历
    val map: RDD[String] = rs.map( row => row.getString(1) )
    map.foreach( println )

  }
}
