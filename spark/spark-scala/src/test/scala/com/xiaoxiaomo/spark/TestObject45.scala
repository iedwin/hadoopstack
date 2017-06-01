package com.xiaoxiaomo.spark

/**
 * 伴生对象
 */
class TestObject45{
  def display() ={
    TestObject45.value
  }
}

//伴生对象  该对象作为同名类的伴生对象  同名类可以访问伴生对象中的私有变量
object TestObject45{
  private var value = 100
}