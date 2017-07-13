package com.xiaoxiaomo.spark.demo.ch03

import scala.collection.mutable.ArrayBuffer

/**
  * 定长数组数组
  *
  * Scala中的Array以Java中的Array方式实现
  *
  * Created by xiaoxiaomo on 2016/3/23.
  */
object ArrayFixedLengthDemo {

    def main(args: Array[String]) {

        //1. 定长数组的声明，
        val arr01 = new Array[Int](10);
        val arr02 = new Array[String](12);

        //可以看出：复杂对象类型在数组定义时被初始化为null，数值型被初始化为0
        println(arr01)
        println(arr02)


        //数组元素赋值l
        arr02(0) = "First Element"
        //需要注意的是，val strArray=new Array[String](10)
        //这意味着strArray不能被改变，但数组内容是可以改变的
        println(arr02)


        //2. 另一种定长数组定义方式
        //这种调用方式其实是调用其apply方法进行数组创建操作
        val arr03 = Array("he", "llo,", "wor", "d", "!")
        println(arr03)
    }


}
