package com.xiaoxiaomo.spark.practice

/**
  * Created by TangXD on 2017/9/20.
  */
object T8_Seq {

    def main(args: Array[String]): Unit = {
        var seq = Seq[String]()
        seq = seq :+ "hello"
        seq = seq :+ "word"

        println(seq)

        seq :+ "hello"
        println(seq)
    }

}
