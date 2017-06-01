package com.xiaoxiaomo.spark.demo.ch03

import scala.collection.mutable.ArrayBuffer

/**
 * 数组
 * Created by Jason on 2016/3/23.
 */
class AarryDemo {

    //1. 定长数组的声明
    val arr01 = new Array[Int](10) ;
    var arr02 = new Array[String](12) ;
    val arr03 = Array("he","llo,","wor","d","!")

    //2. 变长数组
    var arr04 = new ArrayBuffer[Int]() ;
}
