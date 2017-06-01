package com.xiaoxiaomo.spark.sql

import java.sql.{DriverManager, PreparedStatement}

/**
  * Spark SQL 读取hive
  *
  * 运行：
  * /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.sql.SparkSQLJDBC spark_scala-1.0-SNAPSHOT.jar
  *
  * Created by xiaoxiaomo on 2016/11/17.
  */
object SparkSQLJDBC {

  //注意用户，不然会有异常
  // User anonymous does not have privileges for QUERY
  def main(args: Array[String]) {

    //1. 加载驱动
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://10.141.4.204:10000/default","root","")

    //2. 操作
    val statement: PreparedStatement = conn.prepareStatement("select * from test_1")
    val rs = statement.executeQuery()
    while(rs.next()){
      println(rs.getString(1))
    }

    //3. 关闭连接
    rs.close
    statement.close()
    conn.close
  }
}