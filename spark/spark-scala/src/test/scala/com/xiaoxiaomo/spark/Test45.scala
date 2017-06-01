package com.xiaoxiaomo.spark

object Test45{
  def main(args: Array[String]): Unit = {
    //在java中，类实例化称之为对象
    //scala中对象使用object表示，对象是一种单独的类型
    //在scala不在有静态字段和静态方法定义，使用object实现
//    println(TestObject45.value)
    var c = new TestObject45()
    println(c.display())
  }
}