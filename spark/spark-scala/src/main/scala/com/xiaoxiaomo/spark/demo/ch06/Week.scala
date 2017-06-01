package com.xiaoxiaomo.spark.demo.ch06

/**
 * 枚举
 * Created by xiaoxiaomo on 2016/3/29.
 */
object Week extends Enumeration{
    //初始化
    var Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday = Value

    //或则
    val M = Value
    val T = Value
    val W = Value

    //调用Value方法设置值id或者name
    //id为Int，name为String
    val M2 = Value(30,"M2")
    val M3 = Value(40)//可以只设置id或name
    val M4 = Value("M4name")//Id默认为11,在之前+1

}

object testEnume{

    def main(args: Array[String]) {
        val Mon = Week.Monday //获取值
        val M4Id = Week.withName("M4name") //获取Id
        val m4 = Week(41)

        //遍历
        for ( w <- Week.values ){
            printf("id:%s , value:%s",w.id,w)
            println ("")
        }


        println("Mon:"+Mon)
        println("M4Id:"+M4Id)
        println("m4:"+m4)
    }
}
