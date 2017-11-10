package com.xiaoxiaomo.spark.practice

/**
  * Created by TangXD on 2017/9/16.
  */
object T7_map_flatMap {

    def main(args: Array[String]): Unit = {

        val res1 = List(1,2,3,4).map( _+1 )


        val words = List("zks","zhaikaishun","kaishun","kai","xiaozhai")

        println( words.map(_.length) )

        println(words.map(_.toList))
        println(words.flatMap(_.toList))
        println( res1 )
        println( "=========================================================" )

        val tuples = List.range(1, 5).flatMap(i => List.range(1, i).map(j => (i, j)))

        println(tuples)


    }

}
