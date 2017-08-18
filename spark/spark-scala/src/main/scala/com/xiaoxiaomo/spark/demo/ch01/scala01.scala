package com.xiaoxiaomo.spark.demo.ch01

import scala.util.Random

/**
  *
  * 第一章
  * 1. 变量的声明 var 变量名：[类型] = 值
  * 2. 常量的声明 val 变量名：[类型] = 值
  * 3. 方法的调用
  * 4. apply 方法
  *
  * Created by xiaoxiaomo 2016/3/23.
  */
object scala01 {


    //1. 变量的声明
    var a = 17 ;
    var b:Int = 18 ;
    var c:String = "this is string" ;
    var a1 = 17 ;

    //2. 常量的声明
    val d = 17 ;
    val e:Double = 20.00 ;
    val f:Float = 21f ;
    val d1 = 17 ;

    //3. 调用函数和方法
    def main(args: Array[String]) {
        val s = Math.min(89,23) ;   //java中的方法
        val a = Math.abs(-134) ;


        //scala中的min函数
        //其实还是调用的java的Math.min
        //def min(x: Int, y: Int): Int = java.lang.Math.min(x, y)
        val s1 = math.min(89,23) ;


        println(s+" "+a) ;
        println(s1) ;
        println("s == s1:"+s == s1)            //true：常量

        println("a==a1："+a==a1)              //false
        println("d==d1："+d==d1)              //true：常量
        println("a==d："+a==d)               //false


        //apply 方法
//        Scala 是构建在 JVM 上的静态类型的脚本语言，而脚本语言总是会有些约定来增强灵活性。
//        灵活性可以让掌握了它的人如鱼得水，也会让初学者不知所措。比如说 Scala 为配合 DSL 在方法调用时有这么一条约定：
//
//        在明确了方法调用的接收者的情况下，若方法只有一个参数时，调用的时候就可以省略点及括号。
//        如 “0 to 2”，实际完整调用是 “0.to(2)”。但 “println(2)” 不能写成 “println 10”，
//        因为未写出方法调用的接收者 Console，所以可以写成 “Console println 10”
//        https://unmi.cc/scala-apply-update-methods/

        //从0开始
        println(c(2));  /*==等等于 */       println(c.apply(2))

        //伴生对象
        println(BigInt("1289048203"));  /*==等等于 */ println( BigInt.apply("1289048203"))


        val rand = math.BigInt.probablePrime(100 , Random)

        println(rand)

    }





}
