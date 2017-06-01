/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.{Logging, SparkException}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * A stream of {@link org.apache.spark.streaming.kafka.KafkaRDD} where
  * each given Kafka topic/partition corresponds to an RDD partition.
  * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
  * of messages
  * per second that each '''partition''' will accept.
  * Starting offsets are specified in advance,
  * and this DStream is not responsible for committing offsets,
  * so that you can control exactly-once semantics.
  * For an easy interface to Kafka-managed offsets,
  * see {@link org.apache.spark.streaming.kafka.KafkaCluster}
  *
  * @param kafkaParams    Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
  *                       configuration parameters</a>.
  *                       Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
  *                       NOT zookeeper servers, specified in host1:port1,host2:port2 form.
  * @param fromOffsets    per-topic/partition Kafka offsets defining the (inclusive)
  *                       starting point of the stream
  * @param messageHandler function for translating each message into the desired type
  */
private[streaming]
class DirectKafkaInputDStream[
K: ClassTag,
V: ClassTag,
U <: Decoder[K] : ClassTag,
T <: Decoder[V] : ClassTag,
R: ClassTag](
                    ssc_ : StreamingContext,
                    val kafkaParams: Map[String, String],
                    val fromOffsets: Map[TopicAndPartition, Long],
                    messageHandler: MessageAndMetadata[K, V] => R
            ) extends InputDStream[R](ssc_) with Logging {
    val maxRetries = context.sparkContext.getConf.getInt(
        "spark.streaming.kafka.maxRetries", 1)

    // Keep this consistent with how other streams are named (e.g. "Flume polling stream [2]")
    private[streaming] override def name: String = s"Kafka direct stream [$id]"

    protected[streaming] override val checkpointData =
        new DirectKafkaInputDStreamCheckpointData


    /**
      * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
      */
    override protected[streaming] val rateController: Option[RateController] = {
        if (RateController.isBackPressureEnabled(ssc.conf)) {
            Some(new DirectKafkaRateController(id,
                RateEstimator.create(ssc.conf, context.graph.batchDuration)))
        } else {
            None
        }
    }

    protected val kc = new KafkaCluster(kafkaParams)

    private val maxRateLimitPerPartition: Int = context.sparkContext.getConf.getInt(
        "spark.streaming.kafka.maxRatePerPartition", 0)

    /**
      *
      * 获取某个partition能消费到的message的数量
      *
      * 该方法首先会计算一个每分区每秒钟消费的消息数上线effectiveRateLimitPerPartition，
      * 他的value如下图红框中，是在spark.streaming.kafka.maxRatePerPartition和batckpressure中取一个最小值，
      * 如果只配置了一个则以配置的为准，都没配置则返回None，返回None时直接取leader最新的offset。
      * 然后再根据batchTime计算出某partition在batchTime内能消费的消息数上限。
      *
      *
      *
      * @return
      */
    protected def maxMessagesPerPartition: Option[Long] = {
        val estimatedRateLimit = rateController.map(_.getLatestRate().toInt)
        val numPartitions = currentOffsets.keys.size

        val effectiveRateLimitPerPartition = estimatedRateLimit
                .filter(_ > 0)
                .map { limit =>
                    if (maxRateLimitPerPartition > 0) {
                        Math.min(maxRateLimitPerPartition, (limit / numPartitions))
                    } else {
                        limit / numPartitions
                    }
                }.getOrElse(maxRateLimitPerPartition)

        if (effectiveRateLimitPerPartition > 0) {
            val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
            Some((secsPerBatch * effectiveRateLimitPerPartition).toLong)
        } else {
            None
        }
    }

    protected var currentOffsets = fromOffsets

    /**
      * 获取当前InputDStream所包含的topic下所有的partition的最新offset
      *
      * @param retries
      * @return
      */
    @tailrec
    protected final def latestLeaderOffsets(retries: Int): Map[TopicAndPartition, LeaderOffset] = {
        val o = kc.getLatestLeaderOffsets(currentOffsets.keySet)
        // Either.fold would confuse @tailrec, do it manually
        if (o.isLeft) {
            val err = o.left.get.toString
            if (retries <= 0) {
                throw new SparkException(err)
            } else {
                log.error(err)
                Thread.sleep(kc.config.refreshLeaderBackoffMs)
                latestLeaderOffsets(retries - 1)
            }
        } else {
            o.right.get
        }
    }

    /**
      * limits the maximum number of messages per partition
      *
      * Clamp方法是根据Spark.streaming.kafka.maxRatePerPartition和backpressure
      * 这两个参数来设置当前block可以消费到的offset的（即untilOffset），这个数值需要跟partition最新的offset取最小值。
      *
      * @param leaderOffsets
      * @return
      */
    protected def clamp(leaderOffsets: Map[TopicAndPartition, LeaderOffset]): Map[TopicAndPartition, LeaderOffset] = {

        //根据限流大小和backpressure是否开启，获取partition能消费到的offset(untilOffset)
        maxMessagesPerPartition.map { mmp =>
            leaderOffsets.map { case (tp, lo) =>
                //min(当前消费到的offset+要消费的消息数，partition最新的offset)
                tp -> lo.copy(offset = Math.min(currentOffsets(tp) + mmp, lo.offset))
            }
        }.getOrElse(leaderOffsets)
    }

    /**
      * compute方法定义了InputDStream是如何根据指定的batchTime生成RDD的。
      *
      * @param validTime
      * @return
      */
    override def compute(validTime: Time): Option[KafkaRDD[K, V, U, T, R]] = {

        //根据配置获取validTime这个batch对应的每个partition的untilOffsets
        val untilOffsets = clamp(latestLeaderOffsets(maxRetries))

        //封装RDD,定义KafkaRDD的partitions、PreferredLocations、获取数据的方式
        //根据当前InputDStream所消费的topic的每个partition的currentOffset和untilOffset构建KafkaRDD
        val rdd = KafkaRDD[K, V, U, T, R](
            context.sparkContext, kafkaParams, currentOffsets, untilOffsets, messageHandler)

        // Report the record number and metadata of this batch interval to InputInfoTracker.
        val offsetRanges = currentOffsets.map { case (tp, fo) =>
            val uo = untilOffsets(tp)
            OffsetRange(tp.topic, tp.partition, fo, uo.offset)
        }
        val description = offsetRanges.filter { offsetRange =>
            // Don't display empty ranges.
            offsetRange.fromOffset != offsetRange.untilOffset
        }.map { offsetRange =>
            s"topic: ${offsetRange.topic}\tpartition: ${offsetRange.partition}\t" +
                    s"offsets: ${offsetRange.fromOffset} to ${offsetRange.untilOffset}"
        }.mkString("\n")
        // Copy offsetRanges to immutable.List to prevent from being modified by the user
        val metadata = Map(
            "offsets" -> offsetRanges.toList,
            StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
        val inputInfo = StreamInputInfo(id, rdd.count, metadata)
        ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

        currentOffsets = untilOffsets.map(kv => kv._1 -> kv._2.offset)
        Some(rdd)
    }

    override def start(): Unit = {
    }

    def stop(): Unit = {
    }

    private[streaming]
    class DirectKafkaInputDStreamCheckpointData extends DStreamCheckpointData(this) {
        def batchForTime: mutable.HashMap[Time, Array[(String, Int, Long, Long)]] = {
            data.asInstanceOf[mutable.HashMap[Time, Array[OffsetRange.OffsetRangeTuple]]]
        }

        override def update(time: Time) {
            batchForTime.clear()
            generatedRDDs.foreach { kv =>
                val a = kv._2.asInstanceOf[KafkaRDD[K, V, U, T, R]].offsetRanges.map(_.toTuple).toArray
                batchForTime += kv._1 -> a
            }
        }

        override def cleanup(time: Time) {}

        override def restore() {
            // this is assuming that the topics don't change during execution, which is true currently
            val topics = fromOffsets.keySet
            val leaders = KafkaCluster.checkErrors(kc.findLeaders(topics))

            batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
                logInfo(s"Restoring KafkaRDD for time $t ${b.mkString("[", ", ", "]")}")
                generatedRDDs += t -> new KafkaRDD[K, V, U, T, R](
                    context.sparkContext, kafkaParams, b.map(OffsetRange(_)), leaders, messageHandler)
            }
        }
    }

    /**
      * A RateController to retrieve the rate from RateEstimator.
      */
    private[streaming] class DirectKafkaRateController(id: Int, estimator: RateEstimator)
            extends RateController(id, estimator) {
        override def publish(rate: Long): Unit = ()
    }

}
