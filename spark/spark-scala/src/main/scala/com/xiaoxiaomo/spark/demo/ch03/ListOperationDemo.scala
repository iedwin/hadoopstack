package com.xiaoxiaomo.spark.demo.ch03

/**
  * List 操作
  *
  *
  * Created by xiaoxiaomo on 2016/3/23.
  */
object ListOperationDemo {


    def main(args: Array[String]) {

         val nums: List[Int] = 1 :: 2 :: 3 :: 4 :: Nil

        //判断是否为空
//        nums.isEmpty

        //取第一个无素
//        nums.head

        //取除第一个元素外剩余的元素，返回的是列表
//        nums.tail

        //取列表第二个元素
//        nums.tail.head

        //插入排序算法实现
        def isort(xs: List[Int]): List[Int] =
            if (xs.isEmpty) Nil
            else insert(xs.head, isort(xs.tail))

        def insert(x: Int, xs: List[Int]): List[Int] =
            if (xs.isEmpty || x <= xs.head) x :: xs
            else xs.head :: insert(x, xs.tail)

        //List连接操作
        List(1, 2, 3) ::: List(4, 5, 6)

        //取除最后一个元素外的元素，返回的是列表
//        nums.init

        //取列表最后一个元素
//        nums.last

        //列表元素倒置
//        nums.reverse

        //一些好玩的方法调用
//        nums.reverse.reverse == nums

//        nums.reverse.init

//        nums.tail.reverse

        //丢弃前n个元素
//        nums drop 3

//        nums drop 1

        //获取前n个元素
//        nums take 1

//        nums.take(3)

        //将列表进行分割
//        nums.splitAt(2)

        //前一个操作与下列语句等同
//        (nums.take(2), nums.drop(2))

        //Zip操作
        val nums1 = List(1, 2, 3, 4)

        val chars = List('1', '2', '3', '4')

        //返回的是List类型的元组(Tuple）
//        nums zip chars

        //List toString方法
//        nums.toString

        //List mkString方法
//        nums.mkString

        //转换成数组
//        nums.toArray

    }


}
