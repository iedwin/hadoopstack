package com.xiaoxiaomo.spark.practice

/**
 * 方法、函数
 * Created by xiaoxiaomo on 2016/6/15.
 */
object T1_Method {

  def main(args: Array[String]) {
    println( add ( 10,20 ) )
    println( add2( 10,20 ) )
    println( add3( 10,20 ) )
  }

  /**
   * 方式一
   * @param a
   * @param b
   * @return
   */
  def add( a:Int , b:Int ): Int ={
    a+b
  }

  /**
   * 方式二
   * @param a
   * @param b
   * @return
   */
  def add2( a:Int , b:Int ) = {
    a+b
  }

  def add3( a:Int ,b :Int ) = a+b

}
