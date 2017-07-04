package com.xiaoxiaomo.spark.demo.ch03

/**
  *
  * 数组转换
  *
  * Created by xiaoxiaomo on 2016/3/23.
  */
object ArrayTransformDemo {

    def main(args: Array[String]) {

        //定义一个整型数组
        val intArr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)


        //缓冲数据转换后产生的仍然是缓冲数组
        val intArrayVar2 = for (i <- intArr) yield i * 2
        println(intArrayVar2)

        //定长数组转转后产生的仍然是定长数组，原数组不变
        val intArrayNoBuffer = Array(1, 2, 3)
        println(intArrayNoBuffer)


        val intArrayNoBuffer2 = for (i <- intArrayNoBuffer) yield i * 2
        println(intArrayNoBuffer2)


        //加入过滤条件
        val intArrayNoBuffer3 = for (i <- intArrayNoBuffer if i >= 2) yield i * 2
        println(intArrayNoBuffer3)

    }


}
