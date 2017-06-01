package com.xiaoxiaomo.spark.demo.ch06

/**
 * 伴生对象
 * Created by xiaoxiaomo on 2016/3/29.
 */
class Account {

    //调用伴生对象，即使是私有的
    var id = Account.newUniqueNuber()

    private var balance = 0.0

    def deposit(amount:Double): Unit ={
        balance += amount
    }

    //这个不是伴生对象，newUniqueNumber为private
    //然后这里调用就会失败，
    //Accounts.newUniqueNuberPri() ;
}

//伴生对象
object Account{

    private var lastNumber = 0 ;

    //在该Account作用范围之外无法调用
    private def newUniqueNuberPri() = {
        lastNumber  += 1
        lastNumber
    }

    def newUniqueNuber() = {
        lastNumber  += 1
        lastNumber
    }
}

object HelloWorld extends App{
    println("Hello World!")
}