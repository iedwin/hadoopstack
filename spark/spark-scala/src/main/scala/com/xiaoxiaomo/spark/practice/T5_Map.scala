package com.xiaoxiaomo.spark.practice

/**
 * 映射
 * Created by xiaoxiaomo on 2016/6/15.
 */
object T5_Map {

  def main(args: Array[String]) {

    //1. 简单的映射
    val map: Map[String, Int] = Map[ String , Int ]( "a" -> 1 , "b" -> 2 , "c" -> 3 )
//    for ( k <- map.keySet ){
//      println( k  + " " + map(k) )
//    }

    //2. 继续遍历
//    for( k <- map.keysIterator ){ //map.keys
//      println( k  + " " + map(k) )
//    }

    //3. 遍历（k,v）
//    for( (k,v) <- map  ){
//      println( k + " " + v )
//    }

    //4. 修改值,可变Map
    val map1: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.Map[ String , Int ]( "a" -> 1 , "b" -> 2 , "c" -> 3 )
//    map( "a" ) = 5 //默认不导包为不可变数组scala.collection.immutable.Map
    map1( "a" ) = 5
    map1 += ( "d" -> 4 )
    map1 += ( "c" -> 33 )

    for( (k,v) <- map1  ){
      println( k + " " + v )
    }

  }
}
