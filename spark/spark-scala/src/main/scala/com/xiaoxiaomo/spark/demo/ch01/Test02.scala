package com.xiaoxiaomo.spark.demo.ch01

/**
  * 调用犯方法
  * Created by xiaoxiaomo 2016/3/22.
  */
object Test02 {

  def main (args: Array[String]){

    val str = "Hello Word" ;
    println(str)

    val a = min(23, 78);
    print(a) ;



    val b = 3 ;
    while ( b != 0 ){
//      b -= 1 ;
      println( b )
    }

  }

  def min (a:Int , b:Int) :Int = {
    if( a > b ){
      return b ;
    }
    return a ;
  }

}
