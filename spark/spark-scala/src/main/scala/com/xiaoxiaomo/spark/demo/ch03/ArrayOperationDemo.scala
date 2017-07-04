package com.xiaoxiaomo.spark.demo.ch03

import scala.collection.mutable.ArrayBuffer

/**
  * 数组操作
  *
  * Created by xiaoxiaomo on 2016/3/23.
  */
object ArrayOperationDemo {

    def main(args: Array[String]) {

        //定义一个整型数组
        val intArr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

        //求和
        println(intArr.sum)

        //求最大值
        println(intArr.max)

        println(ArrayBuffer("Hello", "Hell", "Hey", "Happy").max)


        //求最小值
        println(intArr.min)

        //toString()方法
        println(intArr.toString())

        //mkString()方法
        println(intArr.mkString(","))

        println(intArr.mkString("<", ",", ">"))

    }

}
