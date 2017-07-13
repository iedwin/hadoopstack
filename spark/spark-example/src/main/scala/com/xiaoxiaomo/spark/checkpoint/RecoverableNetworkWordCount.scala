package com.xiaoxiaomo.spark.checkpoint

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}
import org.apache.spark.util.IntParam

/**
  *
  * To run this on your local machine, you need to first run a Netcat server
  *
  *      `$ nc -lk 9999`
  *
  * and run the example as
  *
  *      `$ java -cp spark-example-1.0.0.jar com.xiaoxiaomo.spark.checkpoint.RecoverableNetworkWordCount localhost 9999 /user/root/checkpoint/ /user/root/out`
  *
  * If the directory ~/checkpoint/ does not exist (e.g. running for the first time),
  * it will create a new StreamingContext (will print "Creating new context" to the console).
  *
  * Otherwise,
  *  if checkpoint data exists in ~/checkpoint/, then it will create StreamingContext from the checkpoint data.
  *
  */
object RecoverableNetworkWordCount {

    def createContext(ip: String, port: Int, outputPath: String, checkpointDirectory: String)
    : StreamingContext = {


        //程序第一运行时会创建该条语句，如果应用程序失败，则会从checkpoint中恢复，该条语句不会执行
        println("Creating new context")

        val outputFile = new File(outputPath)
        if (outputFile.exists()) outputFile.delete()


        val sparkConf = new SparkConf().setAppName("RecoverableNetworkWordCount").setMaster("local[4]")
        // Create the context with a 1 second batch size
        val ssc = new StreamingContext(sparkConf, Seconds(1))

        ssc.checkpoint(checkpointDirectory)

        //将socket作为数据源
        val lines = ssc.socketTextStream(ip, port)
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCounts.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
            val counts = "Counts at time " + time + " " + rdd.collect().mkString("[", ", ", "]")
            println(counts)
            println("Appending to " + outputFile.getAbsolutePath)
            Files.append(counts + "\n", outputFile, Charset.defaultCharset())
        })
        ssc
    }

    //将String转换成Int
    private object IntParam {
        def unapply(str: String): Option[Int] = {
            try {
                Some(str.toInt)
            } catch {
                case e: NumberFormatException => None
            }
        }
    }



    def main(args: Array[String]) {
        if (args.length != 4) {
            System.err.println("参数应该是： " + args.mkString("[", ", ", "]"))
            System.err.println(
                """
                    |Usage: RecoverableNetworkWordCount <hostname> <port> <checkpoint-directory> <output-file>.
                    |     <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
                    |     <checkpoint-directory> directory to HDFS-compatible file system which checkpoint data
                    |     <output-file> file to which the word counts will be appended
                    |
                """.stripMargin
            )
            System.exit(1)
        }


        val Array(ip, IntParam(port), checkpointDirectory, outputPath) = args


        //getOrCreate方法，从checkpoint中重新创建StreamingContext对象或新创建一个StreamingContext对象
        val ssc = StreamingContext.getOrCreate(checkpointDirectory,
            () => {
                createContext(ip, port, outputPath, checkpointDirectory)
            })
        ssc.start()
        ssc.awaitTermination()
    }
}
