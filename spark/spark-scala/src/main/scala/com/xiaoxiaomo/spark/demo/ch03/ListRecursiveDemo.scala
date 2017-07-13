package com.xiaoxiaomo.spark.demo.ch03

/**
  *
  * List类型定义及List的特点
  *
  * Created by xiaoxiaomo on 2016/3/23.
  */
object ListRecursiveDemo {

    def main(args: Array[String]) {

        val listStr1: List[Object] = List("This", "Is", "Covariant", "Example")
        println(listStr1)

        //空的List，其类型为Nothing,Nothing在Scala的继承层次中的最低层
        //，即Nothing是任何Scala其它类型如String,Object等的子类
        val listStr2 = List()
        println(listStr2)

        val listStr3: List[String] = List()
        println(listStr3)


        //采用::及Nil进行列表构建
        val nums1 = 1 :: (2 :: (3 :: (4 :: Nil)))
        println(nums1)

        //由于::操作符的优先级是从右往左的，因此上一条语句等同于下面这条语句
        val nums2 = 1 :: 2 :: 3 :: 4 :: Nil
        println(nums2)
    }


}
