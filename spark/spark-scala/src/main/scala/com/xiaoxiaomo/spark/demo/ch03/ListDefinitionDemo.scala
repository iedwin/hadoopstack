package com.xiaoxiaomo.spark.demo.ch03

/**
  *
  * List类型定义及List的特点
  *
  * Created by xiaoxiaomo on 2016/3/23.
  */
object ListDefinitionDemo {

    def main(args: Array[String]) {

        val fruit = List("Apple", "Banana", "Orange")
        println(fruit)

        //前一个语句与下面语句等同
        val fruit2 = List.apply("Apple", "Banana", "Orange")
        println(fruit2)


        //数值类型List
        val nums = List(1, 2, 3, 4, 5)
        println(nums)


        //多重List，List的子元素为List
        val diagMatrix = List(List(1, 0, 0), List(0, 1, 0), List(0, 0, 1))
        println(diagMatrix)

        //遍历List
        for (i <- nums) println("List : " + i)

        //List一但创建，其值不能被改变
        //nums(2)=7
    }


}
