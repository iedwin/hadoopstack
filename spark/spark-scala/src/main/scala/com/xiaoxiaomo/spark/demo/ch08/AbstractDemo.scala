package com.xiaoxiaomo.spark.demo.ch08

/**
 * 抽象类
 * Created by xiaoxiaomo on 2016/4/4.
 */
abstract class AbstractDemo {

    //没有初始化值，这是一个带有get方法的抽象字段
    val id:Int
    //没有初始化值，这是一个带有get/set方法的抽象字段
    var age:Int

    //不需要使用override
    def name:Unit ; //没有方法体

    //不需要使用override
    //带参数的方法
    def sayHi( who:String ) ; //没有方法体

    //不需要使用override
    //带参数和返回值(Int)的方法
    def sum( a:Int ,b:Int ) : Int; //没有方法体

    //这不是一个抽象方法而是一个普通方法
    def getOther(): Unit ={
        println("这个方法自己有方法体")//有方法体
    }
}

class AbstractImp extends AbstractDemo{

    val id: Int = 17

    //不需要使用override
    def sum(a: Int, b: Int): Int = {
        a + b
    }

    //不需要使用override
    def name = println("xiaomo")

    //不需要使用override
    def sayHi(who: String): Unit = {
        println("Hello everyone! My name is "+who + " And "+ age + " years old！")
        getOther() ;//调用自己的方法
    }

    var age: Int = 25

    //这不是一个抽象方法而是一个普通方法
    override def getOther(): Unit = super.getOther()
}

object Test3 extends App{

    val abs = new AbstractImp()
    abs.name
    println(abs.sum(7,8)) //调用求和方法
    abs.sayHi("xiaoxiaomo")//sayHi方法
}
