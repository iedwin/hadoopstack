package com.xiaoxiaomo.spark.demo.ch09

import java.io.File

import scala.io.{Source, BufferedSource}

/**
 * 根据正则读取词法单元
 * Created by xiaoxiaomo on 2016/4/6.
 */
object ReadSqlit {

    def main(args: Array[String]) {

        //通过相对路径读取资源文件
        val source = Source.fromFile(new File("test.txt"),"UTF-8")

        //通过空格（正则表达式）切分成一个一个数据
        val word = source.mkString.split("\\s+")

        //遍历数据
        for( w <- word ){
            println(w)
        }
    }
}
