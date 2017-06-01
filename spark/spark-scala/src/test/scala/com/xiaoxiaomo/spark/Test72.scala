package com.xiaoxiaomo.spark

/**
 * 继承--构造顺序
 */

class Person1{
  val name:String = "zhangsan"
  val age:Int = 10
  lazy val interest:Array[Int] = new Array[Int](age) 
  println("Person1类")
}

class Student1 extends Person1{
  override val age:Int = 20
  println("Student1类")
}

object Test72 {
  def main(args: Array[String]): Unit = {
    val c = new Student1
//    println(c.age)     //20
//    println(c.name)
    println(c.interest.length)   //0
    //说明：
    //1、开始实例化Student1对象时，先调用父类的构造器，初始化父类字段值
    //2、在父类初始化的过程中，父类发现子类需要覆盖父类的age字段，此时getter方法没有初始化
    //3、开始初始化interest字段，需要调用age字段的getter方法，发现getter方法没有分配值，将0作为interest数组的初始化长度
    //4、子类的age字段开始初始化，生成对应的getter方法，age初始化为20
    //5、解决办法：把需要初始化的字段添加lazy,在构造的最后在调用初始化操作
    
  }
}