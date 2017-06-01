package com.xiaoxiaomo.spark.demo.ch01

/**
  *
  * 数组与for循环
  *
  * Created by xiaoxiaomo on 2016/6/14.
  */
object Test03 extends App{
//  println( "blog.xiaoxiaomo.com" )
  var people = Array("dog","cat","pig","cj")
  val s = 3 ;
  for ( i <- 0 until people.length; if i % s == 0 ){
    println( people(i) )
  }
}

