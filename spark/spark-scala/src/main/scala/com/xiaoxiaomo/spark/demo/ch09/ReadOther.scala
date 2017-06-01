package com.xiaoxiaomo.spark.demo.ch09

import scala.io.{BufferedSource, Source}

/**
 * 从URL或其他源读取数据
 * Created by xiaoxiaomo on 2016/4/5.
 */
object ReadOther {

    //TEST
    def main(args: Array[String]) {

        //从URL中读取信息
        //注意URL中的编码
        var rL1: BufferedSource = Source.fromURL("http://blog.xiaoxiaomo.com/2016/04/02/Scala-%E7%BB%A7%E6%89%BF/")


        //从String字符串中读取
        val string: Source = Source.fromString("blog.xiaoxiaomo.com")
        val str = string.getLines().mkString

        //从Char中读取信息
        Source.fromChar(str.charAt(2)) ;

        //从Byte数组中读取信息
        Source.fromBytes(new Array[Byte](100)) ;

    }

}
