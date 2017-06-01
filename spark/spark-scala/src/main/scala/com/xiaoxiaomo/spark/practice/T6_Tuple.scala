package com.xiaoxiaomo.spark.practice

/**
 * 元祖
 * 对偶也是元祖，一种的元祖特殊
 * Created by xiaoxiaomo on 2016/6/15.
 */
object T6_Tuple {

  def main(args: Array[String]) {
    //1. 简单定义
    //下标从1开始
    val tuple1 = ( "a" , "b" , "c" , "d" ,55 )
//    println( tuple1 _1 )
//    println( tuple1._5 )

    //2. 模式匹配  _元组元素占位符
    val (first,second,third,forth,_) = tuple1
//    println( first )
//    println( forth )

//    println( "Blog".partition( _.isUpper ) )
//    println("Hello World!".partition(_.isUpper))

    //拉链操作
    var arr1 = Array(1,2,3,4,5)
    var arr2 = Array('a','b','c','d')
    var temp = arr2.zip(arr1)
    println(temp.mkString(","))
    var map = temp.toMap
    println(map)
  }

}
