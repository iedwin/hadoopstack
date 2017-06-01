package com.xiaoxiaomo.spark.demo.ch05

/**
 * Created by xiaoxiaomo on 2016/3/28.
 */
class SimpleClazz {

    private var num = 0 ;//必须初始化
    var name = "twosnail" ;//必须初始化

    //默认是一个public方法
    def increase(): Unit ={
        num += 1 ;
    }
    //一个简单的取值方法
    def getIncrease = num ;

}

//测试一下
object TestSimpleClazz{
    def main(args: Array[String]) {
        val simple = new SimpleClazz()
        simple.increase() //建议有“()”
        println( simple.getIncrease ) //可以省略“()”
        //get/set
        simple.name = "xiaoxiaomo"
        print(simple.name)
    }
}