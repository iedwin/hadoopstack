package com.xiaoxiaomo.spark.demo.ch02

/**
 * Created by xiaoxiaomo 2016/3/22.
 */
object ForDemo {

  def main(args: Array[String]) {
    var file  = null;



    //for循环遍历

    val str = "Hello" ;

    for (i <- str){
      println( i )
    }

    for ( i <- 0 until str.length ){
      println( str(i) )
    }




    //针对每一次 for 循环的迭代, yield 会产生一个值，被循环记录下来 (内部实现上，像是一个缓冲区).
    def scalaFiles =
      for {
        file <- "filesHere"
//        if file.isFile
//        if file.getName.endsWith(".scala")
      } yield file


  }




}
