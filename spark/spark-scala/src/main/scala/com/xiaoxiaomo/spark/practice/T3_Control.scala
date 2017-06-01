package com.xiaoxiaomo.spark.practice

/**
 * 流程控制语句while for
 * Created by xiaoxiaomo on 2016/6/15.
 */
object T3_Control {

  def main(args: Array[String]) {

//    while ( true ){
//      println( "hello world" )
//    }

    //循环，开区间
//    for( i <- 1 until 10 ){
//      println( i )
//    }

    //反转
    //println( (1 to 10).reverse ) //Range(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
//    for( i <- (1 to 10).reverse  ){
//      println(  i )
//    }

//    for( i <- 1 to 10 reverse  ){
//      println(  i )
//    }

    //迭代单个单词
//    for( i <- "blog.xiaoxiaomo.com" ){
//      println( i )
//    }

    //多重for循环,和守卫
    for( i <- 1 to 5; if i%2==0 ; j <- 1 to 5 ; if j == 2){
      print( i+ j * 10 + "  ")
    }

    //for推导式
//    val newV =  for( i <- 1 to 10 ; if i%3 ==0 ) yield i
//    println( newV )

//    val v1 = ( 1,2,3,4,5,6,8,9 )
//    for  ( i <- v1 ){
//      print( i + " " )
//    }
  }
}
