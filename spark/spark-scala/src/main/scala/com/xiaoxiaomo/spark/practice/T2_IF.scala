package com.xiaoxiaomo.spark.practice

/**
 * 判断judge
 * Created by xiaoxiaomo on 2016/6/15.
 */
object T2_IF {

  def main(args: Array[String]) {


    println ( judge( 99 ) )
    /**
     * 判断一
     * @return
     */
//    def judge( a:Int ) ={
//      if( a > 0 ) "blog.xiaoxiaomo.com" else -1
//    }

    /**
     * 判断二
     * @param a
     * @return
     */
    def judge( a:Int ) = {
      if( a > 0 ){
        1
      }else if( a==0 ){
        0
      }else{
        -1
      }
    }

  }
}
