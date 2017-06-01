package com.xiaoxiaomo.spark.demo.ch09

import java.io.{PrintWriter, File}

import scala.io.{BufferedSource, Source}

/**
 * 读取文件
 * Created by xiaoxiaomo on 2016/4/5.
 */
object ReadLines {

    //TEST
    def main(args: Array[String]) {
        //读取文件，事例一
        val source: BufferedSource = Source.fromFile(new File("D:\\test.txt"),"UTF-8")
        //读取文件，事例二（new File可以省略，字符编码如果省略默认使用缺省值）
        val source2: BufferedSource = Source.fromFile("D:\\test.txt","UTF-8")

        //遍历的方式一，直接String输出
        //print(source.mkString)

        //遍历的方式二，使用foreach逐行输出
        //source.getLines().foreach(println)

        //遍历的方式三，使用for遍历单个字符输出
        for (c <- source2 ){ //逐个字符读取
            println(c)
        }

        source.close()
        source2.close()
    }
    
}
