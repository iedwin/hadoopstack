package com.xiaoxiaomo.spark

/**
 * 控制结构if
 */
object Test21{
  def main(args: Array[String]): Unit = {
    /*判断if用法一*/
/*    def jugde(a:Int) = {
      if(a > 0) "xiaoxiaomo" else 0  //() ? :
    }

    var c = jugde(-100)
    println(c)*/

    /*判断if用法二*/
/*    def jugde(a:Int) = {  //分层判断
      if(a > 0){
        1
      }else if(a < 0 ){
        -1
      }else{
    	  0
      }
    }
    val c = jugde(100)
    println(c)*/

    /*判断if用法三*/
    def jugde(a:Int) :Int = {  //分层判断
      if(a > 0){
        return 1      //scala中没有break语句，只用return，相当于函数版的break
      }else if(a < 0 ){
        return -1
      }
        0
    }
    val c = jugde(100)
    println(c)
  }
}
