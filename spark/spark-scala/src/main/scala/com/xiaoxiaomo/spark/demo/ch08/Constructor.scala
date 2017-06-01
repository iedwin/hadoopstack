package com.xiaoxiaomo.spark.demo.ch08

/**
 * 继承-构造器
 * Created by xiaoxiaomo on 2016/4/4.
 */
class ConstructorPartDemo(val age :Int) {

    val name  = "xiaoxiaomo"
    val height = 178 ;
    println("父类主构造器")

    //辅助构造器
    def this( name : String, age : Int){
        this(age)//必须在构造函数首行
        println("父类，辅助构造器，name:"+name+" age:"+age+" height:"+height)
    }
}

/**
 * 第一个子类
 * 继承超类的主构造器
 * @param age 传递给父类
 * @param name
 */
class ConstructorSub1(age:Int,override val name:String) extends ConstructorPartDemo(age){

    println("子类1，主构造器")

    //主构造器重已经重写了字段name，下面就不能再次重写
    //override val name  = "xiaoxiaomo"
    //辅助构造器
    def this( name : String, age : Int){
        this(age,name)
        println("子类1，辅助构造器，name:"+name+" age:"+age+" height:"+height)
    }
}

/**
 * 第二个子类
 * 继承超类的辅助构造器
 * @param age 传递给父类
 * @param name 传递给父类
 * @param height
 */
class ConstructorSub2(age:Int,override val name:String,height:Double) extends ConstructorPartDemo(name,age){


    println("子类2，主构造器")

    //辅助构造器
    def this( name : String, age : Int){
        this( age , name , 175 )
        println("子类2，辅助构造器，name:"+name+" age:"+age + " height:"+height)
    }
}

//下面写法是不会通过编译的
//因为没有ConstructorPartDemo()的无参构造器
//class ConstructorSub3(age:Int,override val name:String) extends ConstructorPartDemo(){
//}

//TEST
object Test extends App{
    new ConstructorSub1("one-snail",11)
    println("----------------")
    new ConstructorSub2("two-snail",22)
}