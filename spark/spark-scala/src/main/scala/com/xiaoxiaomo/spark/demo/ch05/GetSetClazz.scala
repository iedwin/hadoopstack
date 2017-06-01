package com.xiaoxiaomo.spark.demo.ch05

import scala.beans.BeanProperty

/**
 * Created by xiaoxiaomo on 2016/3/28.
 */
class GetSetClazz {

    //必须初始化
    //默认共有字段，自带共有get/set方法
    var name = "twosnail"

    //共有字段，由于声明为val不能修改所以只带get方法
    val age = 23

    //私有字段，只有自己能访问的get/set方法
    private var sex = 1

    //私有字段，只有自己能访问的get方法
    private val work = "hadoop工程师"

    //@BeanProperty生成JavaBeans规范get/set
    @BeanProperty var height = 175.00

    def setSex( sex : Int): Unit ={
        if( sex != 0 && sex != 1 ){
            println("性别设置异常！")
        } else this.sex = sex
    }

    def getSex = sex //自定义的方法
    def getWork = work  //自定义方法提供给外部访问

}

//测试一下
object TestGetSetClazz{
    def main(args: Array[String]) {
        val clazz = new GetSetClazz()
        //var name 可以get/set
        clazz.name = "xiaoxiaomo"
        println(clazz.name)
        //val age 只有get
        println(clazz.age)
        //java规范或者普通的
        clazz.setHeight(180.00)//等于 clazz.height = 180.00
        println(clazz.getHeight)//等于 println(clazz.height)

        //调用自定义方法
        clazz.setSex(0)
        println(clazz.getSex)
        println(clazz.getWork)
    }
}