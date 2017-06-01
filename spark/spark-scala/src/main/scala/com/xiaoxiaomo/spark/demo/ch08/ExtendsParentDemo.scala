package com.xiaoxiaomo.spark.demo.ch08

/**
 * 继承
 * Created by xiaoxiaomo on 2016/4/4.
 */
class Parent {
    //父类
    val name = "xiaomo";//通过val重写
    val age = 25 ; //通过val重写
    var height = 175.0 ;//子类没有重写
    private val six = 0 ;//私有字段
    protected val money = 9999999;//protected

    //无参方法
    def supSex = six ;

    //say方法
    def say(info:String){
        println("Parent say ,"+ info)
    }

}

class SubClass extends Parent{ //子类
    //val重写父类val参数
    override val name = "xiaoxiaomo"
    override val age  = 24 ;
    override val money =  99999

    //val重写不带参数的def
    //调用父类方法使用super关键字
    override val supSex: Int = {
        if( super.supSex == 0 )
            1
        else 0
    }

    //def只能重写一个def方法
    //访问父类字段不需要使用supper，直接访问即可
    override def say(info : String): Unit = {
        println("SubClass say ,"+ info + " age:"+age +" name:"+name )
        println("height:"+height + " money:"+money)
    }
}

//Test
object Test2 extends App{
    new SubClass().say("Hello!")
    print(new SubClass().supSex)
}

