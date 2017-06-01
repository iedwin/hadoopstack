package com.xiaoxiaomo.spark.demo.ch06

/**
 * apply方法
 * Created by xiaoxiaomo on 2016/3/29.
 */
class AccountApply ( val id : Int , initialBalance : Double ) {

    //
    private var balance  = initialBalance ;

}

//伴生对象
object AccountApply{

    //apply方法
    def apply( initialBalance : Double ) =
        new AccountApply(Compute.sum(8,9) ,initialBalance) ;

    def main(args: Array[String]) {

        val acct = AccountApply(1000.0) ;
        println(acct)
    }
}