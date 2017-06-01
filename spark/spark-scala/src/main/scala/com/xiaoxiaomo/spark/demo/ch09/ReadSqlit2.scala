package com.xiaoxiaomo.spark.demo.ch09

import java.io.File

import scala.io.{BufferedSource, Source}

/**
  * 切割后处理
  * Created by xiaoxiaomo on 2016/4/6.
  */
object ReadSqlit2 {

     def main(args: Array[String]) {
         val source: BufferedSource = Source.fromFile(new File("test.txt"),"UTF-8")
         val word = source.mkString.split("\\s+")

         //转为Double
         for( w <- word ) yield {
             val double: Double = w.toDouble
             println(double+ " ")
         }

         //或者通过另一种方式转换
         val map: Array[Double] = word.map(_.toDouble)
         for( m <- map ) {
             print(m+ " - ")
         }
     }
}
