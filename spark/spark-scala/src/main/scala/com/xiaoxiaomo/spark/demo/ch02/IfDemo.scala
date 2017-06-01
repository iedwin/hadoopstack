package com.xiaoxiaomo.spark.demo.ch02

/**
 * Created by Jason on 2016/3/22.
 */
object IfDemo {

  def main (args: Array[String]){
    print(min( 78 ,90 )) ;
  }

  /**
   * 获取最小值
   * @param x
   * @param y
   * @return
   */
  def min ( x:Int , y:Int ):Int = {

    if( x > y ){
      return y ;
    }
    return x ;
  }
}
