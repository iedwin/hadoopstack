package com.xiaoxiaomo.spark.demo.ch03

import scala.collection.mutable.ArrayBuffer

/**
  * 定义变长数组数组
  *
  * 要使用ArrayBuffer，先要引入scala.collection.mutable.ArrayBuffer
  *
  * Created by xiaoxiaomo on 2016/3/23.
  */
object ArrayVariableLengthDemo {

    def main(args: Array[String]) {

        val array = ArrayBuffer[String]()
        println(array)
        
        //2. +=意思是在尾部添加元素
        array += "Hello"
        println(array)

        //3. +=后面还可以跟多个元素的集合
        //注意操作后的返回值
        array +=("blog", "xiaoxiaomo", "com")
        println(array)

        //4. ++=用于向数组中追加内容，++=右侧可以是任何集合
        //追加Array数组
        array ++= Array("WellCome", "To", "!")
        println(array)
        
        //5. 追加List
        array ++= List("WellCome", "To", "!")
        println(array)
        
        //6. 删除末尾n个元素
        array.trimEnd(3)
        println(array)

        //在数组索引为0的位置插入元素"HAHA"
        array.insert(0,"HAHA")
        println(array)


        //在数组索引为0的位置插入元素"HAHA","嘿"
        array.insert(0,"HAHA","嘿")
        println(array)


        //转成定长数组
        array.toArray
        println(array)

        //将定长数组转成ArrayBuffer
        array.toBuffer
        println(array)

    }


}
