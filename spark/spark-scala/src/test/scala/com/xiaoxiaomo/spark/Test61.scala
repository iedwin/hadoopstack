package com.xiaoxiaomo.spark

/**
 * 模式匹配
 */
object Test61 {
  def main(args: Array[String]): Unit = {
    
    var value = "xiaoxiaomo*"
    
    var rs = value match {
      case "xiaoxiaomo*" => 1
      case "嘿嘿" => "呵呵"
      case _ if value.contains("*")  => println(value)
      case _ => 0
    }
    println(rs)
  }
}