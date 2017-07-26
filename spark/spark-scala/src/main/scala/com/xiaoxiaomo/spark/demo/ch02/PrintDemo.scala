package com.xiaoxiaomo.spark.demo.ch02

//import scala.io.StdIn
//在2.10.4中没有该方法，在2.11.7中有
//http://www.scala-lang.org/api/2.11.7/index.html#scala.io.StdIn$

/**
 * Created by xiaoxiaomo 2016/3/22.
 */
object PrintDemo {

  def main(args: Array[String]) {
    val name = readLine("your name:");
//    val name01 = StdIn.readLine("your name:")

//    val age = StdIn.readInt() ;


//    printf("Hello , %s！ next year , your will be %d .\n ", name01 , age+1 ) ;
    printf("Hello , %s！ next year , your will be %d .\n ", name , 12 ) ;




  }

}
