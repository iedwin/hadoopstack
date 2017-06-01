package com.xiaoxiaomo.spark.demo.ch06

/**
 * 单例对象
 * Created by xiaoxiaomo on 2016/3/29.
 */
object Compute {
    //最后一个数
    var sums = 0

    def sum( a : Int , b :Int ) : Int =  {
        sums = a + b //不需要添加return
        sums
    }
}

object isSame{
    //test
    def main (args: Array[String]){
        println( Compute.sum(8,9) )
        println( Compute.sums )
        //判断是否是相同对象
        println(Compute == Compute)
    }
}