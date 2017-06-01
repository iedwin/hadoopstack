package com.xiaoxiaomo.spark.demo.ch05

/**
 * 主构造函数
 * Created by xiaoxiaomo on 2016/3/28.
 */
class StructureClazz( val name:String , private var age :Int = 0) {

    println("这是一个构造函数！ name："+ name)

    def addAge = age += 1
    def getAge() = age
}

object TestStructure{
    def main(args: Array[String]) {
        val test = new StructureClazz("xiaoxiaomo")
        println(test.getAge())
    }
}
