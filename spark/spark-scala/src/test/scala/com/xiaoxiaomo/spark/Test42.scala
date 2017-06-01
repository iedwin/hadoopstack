package com.xiaoxiaomo.spark

/**
  * Created by TangXD on 2016/11/16.
  */
object Test42 {
  def main(args: Array[String]): Unit = {
    var c = new TestClass42
     c.setValue(100)

    var c1 =  new TestClass42
    c1 setValue 1000
    c1.add(c)
    println(c1.getValue)  //如果方法名带(),调用时可以加(),也可以不加
                          //如果方法名不带(),调用时不能加()

  }
}
