package com.xiaoxiaomo.spark.demo.ch09

import java.io.{File, FileInputStream}

/**
 * 读取二进制文件
 * Created by xiaoxiaomo on 2016/4/6.
 */
object InputStream {

    def main(args: Array[String]) {
        val file: File = new File("test.txt")
        val in = new FileInputStream(file)

        var c = 0 ;
        while( c != -1 ){
            c = in.read()
            print(c.toChar)
        }
        in.close()
    }
}
