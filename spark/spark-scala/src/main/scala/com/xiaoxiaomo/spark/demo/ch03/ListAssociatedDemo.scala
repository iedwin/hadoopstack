package com.xiaoxiaomo.spark.demo.ch03

/**
  *
  * List伴生对象方法
  *
  * Created by xiaoxiaomo on 2016/3/23.
  */
object ListAssociatedDemo {

    def main(args: Array[String]) {
        //apply方法
        val list = List.apply(1, 2, 3)
        println(list)

        //range方法，构建某一值范围内的List
        println(List.range(2, 6))

        //步长为2
        println(List.range(2, 6,2))

        //步长为-1
        println(List.range(2, 6,-1))

        println(List.range(6,2 ,-1))

        //构建相同元素的List
        val res145 = List.make(5, "hey")
        println(res145)

        //unzip方法
//        List.unzip(res145)

        //list.flatten，将列表平滑成第一个无素
        val xss = List(List('a', 'b'), List('c'), List('d', 'e'))
        println(xss)

        println(xss.flatten)

        //列表连接
        val listc= List.concat(List('a', 'b'), List('c'))
        println(listc)
        
    }


}
