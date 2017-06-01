package com.xiaoxiaomo.spark

/**
 * 继承--抽象类
 */

//定义一个抽象类
abstract class person{
  var id:Int
  val name:String 
  def show:Int
  println("person类")
}

class student extends person{
  //如果父类是抽象类时，覆盖父类的字段时可以写override,也可以不写
  //如果父类不是抽象类时，必须要写override
  override var id:Int = 100   //var定义的抽象字段只能使用var覆盖
  override val name:String = "张三"   //val定义的抽象字段只能使用val进行覆盖
  override def show:Int = 90   //def定义的抽象方法可以使用def或者val覆盖
  println("student类")
}


object Test71 {
  def main(args: Array[String]): Unit = {
    //1、scala中，有抽象字段一定是抽象类
    //2、没有抽象字段可以是普通的类也可以是抽象类
    //3、覆盖父类的字段其实质是覆盖父类的setter和getter方法
    var c = new student
    println(c.show)
  }
}