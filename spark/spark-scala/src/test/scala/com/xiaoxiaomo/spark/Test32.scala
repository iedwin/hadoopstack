package com.xiaoxiaomo.spark

/**
 * 映射
 */
object Test32 {
  def main(args: Array[String]): Unit = {
    //(zhangsan,15000)键值对  用()表示，称作对偶
    //不可变map
    val map1 = Map[String,Int]("zhangsan"->15000,"lisi"->9000,"wangwu"->5000) //定义方式一
    val map2 = Map[String,Int]("zhangsan"->15000,("lisi",9000),("wangwu",5000))
//    for(i <- map2){
//      println(i)
//    }


    //增加代码健壮性 添加判断
//    if(map1.contains("zhangsanfeng")){
//    	println(map1("zhangsanfeng"))
//    }

    //scala的做法
//    println(map1.getOrElse("zhangsanfeng", "null"))

    //可变Map
    val map3 = scala.collection.mutable.Map[String,Int]("zhangsanfeng"->20000,"liubei"->140000)
    map3 += ("zhangsan"->19000)
    map3 += ("zhangsanfeng"->21000)
    map3("zhangsanfeng") = 22000
//    for(i <- map3){

//      println(i)
//    }

//    for(i <- map3.keysIterator){
//      println(map3(i))
//    }

//    for((k,v) <- map3){
//      println(k + "->" + v)
//    }

    var map4 = Map[String,Int]("zhangsan"->15000,"lisi"->9000,"wangwu"->5000)
    map4 += ("zhangsan" -> 16000)
    println(map2)

    var map5 = for((k,v) <- map4)yield(v,k)
    println(map5)
  }
}
