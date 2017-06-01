package com.xiaoxiaomo.spark.demo.ch06

/**
 * 继承
 * Created by xiaoxiaomo on 2016/3/29.
 */
abstract class Person(var name:String = "momo", var age:Int){
    var height = 178
    private val sex = 0
    def info():Unit

    def info2(){
        println("name:"+name+", sex:"+sex)
    }
}

object man extends Person("xiaoxiaomo", 23){
    //实现方法
    def info(){
        println("my name:"+name+", age:"+age +","+height)
    }
}

object woman extends Person("",22){
    //实现方法
    def info(){
        println("my name:"+name+",age:"+age /*, + sex*/)
    }
}

object test{
    def main(args: Array[String]) {
        man.info()
        man.info2()
        woman.info()
    }
}