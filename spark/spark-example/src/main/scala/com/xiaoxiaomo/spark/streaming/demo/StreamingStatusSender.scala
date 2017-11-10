package com.xiaoxiaomo.spark.streaming.demo

import java.io.PrintWriter
import java.net.ServerSocket

import scala.collection.mutable.ListBuffer

/**
  *
  * 连续随机发送
  * 执行：
  *  /opt/cloudera/parcels/CDH/bin/spark-submit --class com.xxo.spark.streaming.StreamingStatusSender original-spark_scala-1.0-SNAPSHOT.jar 9999 1000
  *
  *  Created by xiaoxiaomo on 2016/6/17.
  */
object StreamingStatusSender {

  def generateContent(index: Int) :String = {
    val charList = ListBuffer[Char]()
    for(i <- 65 to 90){// A B C D ...
      charList += i.toChar
    }
    val charArray = charList.toArray
    charArray(index).toString
  }

  //随机数
  def index = {
    import java.util.Random
    val rdm = new Random()
    rdm.nextInt(7)
  }

  def main(args: Array[String]) {
    if(args.length != 2){
      //参数一是发送的端口  参数二是发送的时间间隔  单位是毫秒
      System.err.println("usage: <port> <millisecond>")
      System.exit(1)
    }
    val listener = new ServerSocket(args(0).toInt)
    while(true){
      val socket = listener.accept()
      new Thread(){
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(),true)
          while(true){
            Thread.sleep(args(1).toLong)
            val content = generateContent(index)
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }


}
