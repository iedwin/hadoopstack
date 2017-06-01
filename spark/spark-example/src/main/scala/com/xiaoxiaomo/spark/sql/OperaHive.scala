package com.xiaoxiaomo.spark.sql

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark SQL 操作Hive
  *
  * 运行方式：
  *   1. /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.sql.ReadJsonStr  spark_scala-1.0-SNAPSHOT.jar local
  *   2. /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.sql.ReadJsonStr  spark_scala-1.0-SNAPSHOT.jar yarn-client
  *
  * Created by xiaoxiaomo on 2016/6/18.
  *
  */
object OperaHive {

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf()
    conf.setAppName( "Spark-SQL OperaHive" )
    conf.set("spark.driver.allowMultipleContexts","true")
    var master = "local"
    if( args.length > 0 ){
      master = args{0}
    }
    conf.setMaster(master)

    val sc: SparkContext = new SparkContext(conf)

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sqlContext.sql("LOAD DATA LOCAL INPATH 'spark_scala/src/main/resources/hivedata.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sqlContext.sql("SELECT key, value FROM src ").collect().foreach(println)
  }

}
