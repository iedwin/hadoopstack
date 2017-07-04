package com.xiaoxiaomo.spark.demo.ch03

/**
  *
  * 多维数组
  *
  * Created by xiaoxiaomo on 2017/7/4.
  */
object ArrayMultiDimDemo {

    def main(args: Array[String]) {

        val multiDimArr=Array(Array(1,2,3),Array(2,3,4))
        println(multiDimArr)

        //获取第一行第三列元素
        println( multiDimArr(0)(2) )


        //多维数组的遍历
        for(i <- multiDimArr) println( i.mkString(","))

    }
}
