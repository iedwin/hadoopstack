package com.xiaoxiaomo.spark.demo.ch00

/**
  * 1. 初始化
  *     1.1. 指定初始化大小
  *           new Array[type](num)
  *     1.2. 指定数组具体值
  *           Array(......)
  * 2. 数组下标元素获取使用"()"
  * 3. 数组API:
  *     3.1. a.length
  *     3.2. a.sum
  *     3.3. a.max
  *     3.4. a.min
  *     3.5. a.mkString(...)
  *     3.6. a.sorted
  *
  * Created by xiaoxiaomo on 2016/6/14.
  *
  */
object ArrayStudy {

  def main(args: Array[String]) {
    var array: Array[String] = new Array[String](3)
    for ( i <- array ){ //看一下默认值
      print(i + " ")
    }

    println()

    array(0) = "小小默"
    array(1)= "abc"
    array(2)="xiaoxiaomo"
    for ( i <- array ){//赋值后遍历
      print(i + " ")
    }

    println()
    array(2) = "xxo"
    println (array(2)) //修改值

    var a = Array( 1,2,3,4,6456,642 )
    for ( i <- 0 until( a.length ) ){
      print(" "+ a(i) )
    }
    println()
    println( a.sum )
    println( a.max )
    println( a.min )

    println( a.mkString( " " ) )
    println( a.mkString( ", " ) )
    println( a.mkString( "<",",", ">" ) )

//    val arr1 = Array(9,"a",4,"fsdf",43)
    val arr1 = Array(9,34,4,90,43)
    val arr2 = arr1.sorted
    println( arr2.mkString(",") )

  }

}
