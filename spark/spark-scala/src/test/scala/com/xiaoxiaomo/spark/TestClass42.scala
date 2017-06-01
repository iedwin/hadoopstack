package com.xiaoxiaomo.spark

/**
 * 类的基本内容2
 */
class TestClass42{

	private var value = 100  //使得setter和getter方法私有化

	def setValue(a:Int){
	  if(a > 0)  //自定义业务逻辑
	  this.value = a
	}

	def getValue = {
	  this.value
	}

	def add(a:TestClass42){
	  this.value += a.value
	}
}
