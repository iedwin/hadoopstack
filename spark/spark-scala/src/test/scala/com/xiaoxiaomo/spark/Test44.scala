package com.xiaoxiaomo.spark

/**
  * Created by TangXD on 2016/11/16.
  */
object Test44 {
  def main(args: Array[String]): Unit = {
    val clazz: TestClass44 = new TestClass44()
    println("name=" + clazz.name + " age=" + clazz.age)
    var c = new TestClass44("xiaoming",15)
       println("name=" + c.name + " age=" + c.age)


  }
}
