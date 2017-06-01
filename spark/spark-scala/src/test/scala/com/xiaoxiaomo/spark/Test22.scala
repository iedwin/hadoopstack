package com.xiaoxiaomo.spark

import java.util

/**
 * 控制结构while for
 */
object Test22{
  def main(args: Array[String]): Unit = {
    /*while*/
    /*    while(true){
      println("xiaoxiaomo")
    }*/

    /*基本for循环*/
    //    for(int i = 0 ; i < 10 ; i++)
    //    for(i : List)

    //    println(1 to 10)
    //    for(i <- 1 to 10){   //闭区间
    //      println(i)
    //    }

    //    for(i <- 1 until 10){ //开区间  前闭后开
    //      println(i)
    //    }

    //    for(i <- (1 to 10).reverse){ //倒序输出
    //      println(i)
    //    }

    //    for(i <- "hello"){
    //      println(i)
    //    }

    /*高级for循环*/
    //守卫
    //    for(i <- 1 to 10  ; if i % 2 == 0){ //修改步长一
    //      println(i)
    //    }
    //    for(i <- 1 to (10,3)){  //修改步长二
    //      println(i)
    //    }

//    for (i <- 1 to 10) {//双重for循环一
//      for (j <- 1 to 5) {
//        println(i + "->" + j)
//      }
//    }

//    for (i <- 1 to 10 ; j <- 1 to 5 ;g <- 1 to 5 ) { //双重for循环二
//      println(i + "->" + j + "->" + g)
//    }

    //for的推导式 将一个集合进行操作产生一个新的集合
//    val c = for(i <- 1 to 10)yield {var a = 0; a = (i + 3) * 2; a}
//    println(c)

    //调用java的API
    val list = new util.ArrayList[Int](3)
    list.add(100)
    list.add(200)
    list.add(300)
    for(i <- 0 to list.size() - 1){
      println(list.get(i))
    }
  }
}
