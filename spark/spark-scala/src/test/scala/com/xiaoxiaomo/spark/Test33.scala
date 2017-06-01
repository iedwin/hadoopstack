package com.xiaoxiaomo.spark

/**
 * 元组
 */
object Test33 {
  def main(args: Array[String]): Unit = {
    //元组定义   元组使用() ,对偶是特殊形式的元组
    //元组下标从1开始
    var t = (1,2,3,4,5,6)
//    println(t _6)

    //取值方式二
    var (first,second,third,forth,_,_) = t   //模式匹配方式访问  _元组元素占位符
//    println(second)

//    println("Hello World!".partition(_.isUpper))  //代表每一个元素

    //拉链操作
    var arr1 = Array(1,2,3,4,5)
    var arr2 = Array('a','b','c','d')
    var temp = arr2.zip(arr1)
//    println(temp.mkString(","))
    var map = temp.toMap
    println(map)

  }
}
