package com.xiaoxiaomo.spark

import scala.collection.mutable.ArrayBuffer

/**
 * 数组
 */
object Test31 {
  def main(args: Array[String]): Unit = {
    //数组定义
    //定长数组
    var arr1 = new Array[Int](3)  //[]中指定类型  （）指定长度
//    arr1(1) = 90   //数组是从0开始
//    for(i <- arr1){  //使用for循环进行遍历
//      println(i)
//    }

    var arr2 = Array(1,2,3,44,5,"hello")  //()中表示元素，用于初始化数组
//    for(i <- arr2){
//      println(i)
//    }
//    for(i <- 0 until arr2.length){
//      println(arr2(i))
//    }
//    println(arr2.max)
//    println(arr2.min)
//    println(arr2.sum)
//    println(arr2.mkString(","))   //Arrays.toString(arr) [1,2,3,4]
//    println(arr2.mkString("<",",",">"))   //Arrays.toString(arr) [1,2,3,4]

    //变长数组
    val arr3 = ArrayBuffer(1,2,3,4,5,6,7,8,"")
    val arr4 = ArrayBuffer(10,11)
    arr3 += 9
    arr3 -= 6
    arr3 ++= arr2
//    println(arr3.mkString(","))

    var arr5 = arr2.toBuffer
    arr5 += 8
//    println(arr5.mkString(" "))

    arr3.toArray

    //排序
    var arr6 = ArrayBuffer(1,7,8,10,5)
    var arr7 = arr6.sorted
//    println(arr7.mkString(","))

    var arr8 = Array("zc","sdf","dfasdf","yh")
    var arr9 = arr8.sorted
//    println(arr9.mkString(","))
//    println(arr8.min)

    //多维数组定义
    var arr10 = Array.ofDim[Int](2, 3)
    arr10(0)(0) = 7
    arr10(1)(2) = 7

//    println(arr10(0)(0))
//    println(arr10.length)
    for(i <- 0 until arr10.length ; j <- 0 until arr10(i).length){
      println(arr10(i)(j))
    }


  }
}
