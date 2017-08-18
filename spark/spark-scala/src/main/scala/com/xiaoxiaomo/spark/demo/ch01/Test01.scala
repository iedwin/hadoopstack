package com.xiaoxiaomo.spark.demo.ch01

/**
  * 方法的调用：
  *     1. 有返回值的方法，返回最后一行
  *     2. 无返回值的方法
  * Created by xiaoxiaomo 2016/3/21.
  */
object Test01 {

  def main(args: Array[String]) {
    println( add( 7, 8 ) )
    printHello( "World" )

  }

  def add( a:Int , b:Int ): Int ={
    a+b
  }

  def printHello( str:String ): Unit ={
    print ("Hello "+ str)
  }


}
