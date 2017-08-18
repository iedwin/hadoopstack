package com.xiaoxiaomo.spark

/**
  * Created by xiaoxiaomo on 2016/11/16.
  */
object Test41 {
  def main(args: Array[String]): Unit = {
    //在scala中，如果给class定义一个变量，scala默认会生成一个对应的setter、getter方法
    //对应的setter、getter方法名称分别为value_=、value
    var c = new TestClass41
    c.value_=(1000)
    println(c.value)

    //使用类java的setter和getter方法，添加字段前@BeanProperty
//    c.setValue(10000)
//    println(c.getValue())

    //@BeanProperty和private不同共用

  }
}
