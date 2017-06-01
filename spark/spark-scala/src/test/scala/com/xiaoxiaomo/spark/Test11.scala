package com.xiaoxiaomo.spark

/**
 * 函数
 */
object Test11 {

  def main(args: Array[String]): Unit = {
    //函数定义,无返回值的函数
    /*    def add(a: Int, b: Int) {
      println(a + b) //3
    }
    add(1, 2)*/

    //在scala中 函数返回()，称之为返回Unit类型，类似java中void类型
    def add(a: Int, b: Int) :Unit = {
      a + b  //默认返回最后一行的值
    }
    var c = add(1,3)
    println(c)


  }
}
