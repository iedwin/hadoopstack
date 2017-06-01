package com.xiaoxiaomo.spark

/**
 * 辅助构造器
 */
class TestClass44 {
  var name:String = ""
  var age:Int = 0

  def this(name:String){//辅助构造器一
    this()   //辅助构造器第一行调用主构造器
    this.name = name
  }
  def this(name:String,age:Int){
    this(name)  //辅助构造器的第一行也可以调用其他的辅助构造器
    this.age = age
  }

  println("*name=" + name + " age=" + age)
}
