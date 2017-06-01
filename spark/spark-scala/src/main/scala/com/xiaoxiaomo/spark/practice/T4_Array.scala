package com.xiaoxiaomo.spark.practice

import scala.collection.mutable.ArrayBuffer

/**
 * 数组
 * Created by xiaoxiaomo on 2016/6/15.
 */
object T4_Array {

  def main(args: Array[String]) {

    //定义数组并遍历
    val arr1 = new Array[Int](5);
//    for  ( i <- 0 until  arr1.length ){
//      print( arr1(i) +" ")
//    }

    for  ( i <- 0 until  arr1.length ){
      arr1(i) = i * 10 + i
    }

    println( arr1.min )
    println( arr1.max )
    val reverse: Array[Int] = arr1.reverse
    println( arr1.mkString( "," ) )
    println( reverse.mkString( "," ) )


    //2. 变长数组
    val arrayBuffer = ArrayBuffer(3,"world","blog")
    println( arrayBuffer.mkString(",") )

    val arrayBuffer1 : ArrayBuffer[Any] = arrayBuffer.++=:("xxo")// ++=添加数组对象
    println( arrayBuffer.mkString(",") )
    println( arrayBuffer1.mkString(",") )

    val arrayBuffer2 : ArrayBuffer[Any] = arrayBuffer.+=:("com")// +=添加元素
    println( "==================================================" )
    println( arrayBuffer.mkString(",") )
    println( arrayBuffer2.mkString(",") )

    println( "=====================多维数组=============================" )
    val dArr: Array[Array[Int]] = Array.ofDim[Int](2,3)

    dArr(0)(0) = -1
    dArr(1)(0) = 10
    dArr(0)(1) = 1
    dArr(1)(1) = 11

    for ( i <- 0 until  dArr.length ;  j <- 0 until dArr(i).length ){
      print(dArr(i)(j) + " " )
    }


  }
}
