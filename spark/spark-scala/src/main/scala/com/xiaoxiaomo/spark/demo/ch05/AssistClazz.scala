package com.xiaoxiaomo.spark.demo.ch05

/**
 * 辅助构造器
 * Created by xiaoxiaomo on 2016/3/28.
 */
class AssistClazz( val name:String , private var age :Int = 0) {

    private var sex = 0
    private var height = 175.0

    //第一个辅助构造器
    def this(sex:Int){
        this("xiaoxiaomo")//调用的主构造器
        this.sex = sex

    }

    //第二个辅助构造器
    def this(sex:Int,height:Double){
        this(sex)//调用的上面一个辅助构造器
        this.height = height
    }
}

//测试一下
object TestAssistClazz{
    def main(args: Array[String]) {
        new AssistClazz("twosnail")//调用的主构造器
        new AssistClazz("twosnail",23)//调用主构造器
        new AssistClazz(1)//调用第一个辅助构造器
        new AssistClazz(1,178.0)//调用第二个辅助构造器
    }
}
