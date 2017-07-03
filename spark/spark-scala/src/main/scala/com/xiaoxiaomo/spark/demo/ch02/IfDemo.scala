package com.xiaoxiaomo.spark.demo.ch02

/**
  * if 表达式
  * Created by xiaoxiaomo on 2016/3/22.
  */
object IfDemo {

    def main(args: Array[String]) {
        print(min(78, 90))
    }

    /**
      * 获取最小值
      *
      * @param x
      * @param y
      * @return
      */
    def min(x: Int, y: Int): Int = {

        if (x > y) {
            y
        }
        x
    }
}
