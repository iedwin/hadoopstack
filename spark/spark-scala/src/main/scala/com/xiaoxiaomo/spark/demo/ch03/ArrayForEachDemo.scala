package com.xiaoxiaomo.spark.demo.ch03

import scala.collection.mutable.ArrayBuffer

/**
  * 定长数组数组
  *
  * Scala中的Array以Java中的Array方式实现
  *
  * Created by xiaoxiaomo on 2016/3/23.
  */
object ArrayForEachDemo {

    def main(args: Array[String]) {

        //定义一个待遍历的数组
        val array = ArrayBuffer("HAHA", "嘿", "Hello", "blog", "xiaoxiaomo", "com", "WellCome", "To", "!")

        //to
        for (i <- 0 to array.length - 1) println("Array to: " + array(i))


        //until
        for (i <- 0 until array.length) println("Array until: " + array(i))

        //数组方式（推荐使用）
        for (i <- array) println("Array array: " + i)

        //步长为2
        for (i <- 0 until(array.length, 2)) println("Array 步长: " + array(i))

        //倒序输出
        for (i <- (0 until array.length).reverse) println("Array 倒序: " + array(i))
    }


}
