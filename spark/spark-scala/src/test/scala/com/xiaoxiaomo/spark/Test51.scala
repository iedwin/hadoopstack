package com.xiaoxiaomo.spark

/**
 * 高阶函数
 */
object Test51 {
  def main(args: Array[String]): Unit = {    //函数定义
  def func1(a:Int , b:Int) = {
    a + b
  }    //    println(func1(1,2))  //直接调用函数



    //scala中函数是一等公民，地位高
    def func2 = func1 _  // _指的是用户传递的是一个确切的函数实现  而不是忘记了给函数传参
    var func3 = func1 _
//    println(func2(2,3))    println( func4(2) )

    //    println(func3(2,3))
    
    def func4 = (a:Int) => (a + 4)   //匿名函数  f(x) = y
    println((x:Int) => 2 * 2)
    
    //定义一个可以接收函数的函数
//    def func5(f:(Int) => Int) = {
//      f(1) + 6
//    }
      def func6(a:Int) = {
        a + 1
    }//    }

    //    println(func5(func6 _))  // _传递    函数是可以省略的

    /*    def fun7(f:(Int,Int) => String) = {
      f(1,9) + " xiaoxiaomo"
    }
    def func8(a:Int ,b:Int) = {
      a + b + ""
    }
    println(fun7(func8 _))  */
    def func8(a: Int, b: Int) = {
      a + b + ""
    }
    
    //使用函数产生函数
    def func7(x:Int , f:(Int,Int) => String) = {
      f(x,x)
    }
    //    println(func7(2,func8 _))

    //简写一
    def func5(f: (Int) => Int) = {
      f(1) + 6
    }
/*    println(func5((x:Int) => x + 2)) //形式一
    println(func5((x) => x + 2))  //形式二
    println(func5(x => x + 2))  //形式三
    println(func5(_ + 2))  //形式四
*/    
    //形式四简写条件
    //1、只有一行代码    //3、匿名函数作为参数


    //2、只有一个变量(针对只有一个参数的函数)
    //简写二
    def func9(f:(Int,Int) => Int) = {
      f(1,2)
    }
//    println(func9((x:Int,y:Int) => x + y))  //形式一
//    println(func9(_ + _))  //形式二

    //总结“_”意义
    //1、获取一个元组的元素
    //2、_指的是用户传递的是一个确切的函数实现  而不是忘记了给函数传参
    //3、_ + 2  表示一种类型 每一个元素
    //4、代表一个占位符
    
    //常见的高阶函数
//    var c = (1 to 10).filter(_ % 2 == 0).map { _ * 2 }
//    println(c.mkString(","))
//    (1 to 10).filter(_ % 2 == 0).map ( _ * 2 ).foreach { println _ }
//    (1 to 10).filterNot(_ % 2 == 0).foreach { println _ }
//    var c = (1 to 10).reduceLeft( _ + _)  //1 + 2  3 + 3  6+ 4
//    println(c)
    
//    (1 to 10).map("*" * _).foreach(println)
    
    //举例
    def func10(x:Int) = {
      (x:Int) =>{
        x + 2
      } * 2
    }
//    println(func10(1)(2))
  }
}