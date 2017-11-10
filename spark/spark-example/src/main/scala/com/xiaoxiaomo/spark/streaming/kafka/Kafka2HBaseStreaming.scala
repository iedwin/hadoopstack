package com.xiaoxiaomo.spark.streaming.kafka

import com.xiaoxiaomo.spark.streaming.util.StreamingUtils
import com.xiaoxiaomo.utils.hbase.HBaseUtils
import org.apache.spark.TaskContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

/**
  * spark consumer 消费kafka数据 存入到HBase
  *
  * 参数：
  * 每次处理条数：timeWindow * maxRatePerPartition * partitionNum
  *
  * Created by xiaoxiaomo on 2016/4/24.
  */
object Kafka2HBaseStreaming {

    def main(args: Array[String]) {

        // 1. 接收传入的参数
        val Array(group , topics, timeWindow, maxRatePerPartition) = args

        // 2. 组装InputDStream参数
        val ssc = StreamingUtils.getStreamingContext( StreamingUtils.getSparkContext(this.getClass.getSimpleName, maxRatePerPartition), timeWindow.toInt)

        // 3. 创建Kafka持续读取流
        val messages = StreamingUtils.createDirectStream(ssc,group,topics)

        // 4. 数据操作
        messages.foreachRDD(rdd => {
            val offsetsList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            // data 处理
            rdd.foreachPartition(partitionRecords => {
                // TaskContext 上下文
                val offsetRange: OffsetRange = offsetsList(TaskContext.get.partitionId)

                // 解析PartitionRecords数据，处理数据
                if (offsetRange.topic != null) {
                    HBaseUtils.insert(offsetRange.topic, partitionRecords)
                }

                // updateZkOffset
                StreamingUtils.updateZkOffset(group,offsetRange)
            })
        })

        ssc.start()
        ssc.awaitTermination()
    }

}
