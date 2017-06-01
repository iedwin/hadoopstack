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

package org.apache.spark.streaming.util

import org.apache.spark.Logging
import org.apache.spark.util.{Clock, SystemClock}

/**
  *
  * RecurringTimer是一个定时重复执行高阶函数callback的执行器，他是通过Thread反复执行loop方法实现的，
  * loop方法中只要定时器不被终止，就会反复调用triggerActionForNextInterval方法，
  * 而triggerActionForNextInterval会在特定的时刻（即nextTime）执行callback函数（即入参updateCurrentBuffer函数）。
  * 执行完成之后会在nextTime上增加period作为下一次执行的时刻。
  *
  *
  * 而period方法是什么呢，他就是我们在构建blockIntervalTimer时的入参blockIntervalMs，
  * 也就是streaming性能的一个优化点spark.streaming.blockInterval。
  * 也就是说，这段代码的逻辑是每间隔blockInterval将由consumer消费到的数据切分成一个block。
  * 由此我们可以看到，这个参数是用来将Batch中所接受到的数据以它为时间间隔切分为block，
  *
  * 而在streaming处理数据时，会将block作为一个partition来进行分布式计算，也就是说我们在指定的batchTime中，
  * 根据blockInterval能切出多少个block，就能分成多少个partition，从而决定了streaming处理时的分布式程度。
  *
  *
  * @param clock
  * @param period
  * @param callback
  * @param name
  */
private[streaming]
class RecurringTimer(clock: Clock, period: Long, callback: (Long) => Unit, name: String)
        extends Logging {

    private val thread = new Thread("RecurringTimer - " + name) {
        setDaemon(true)

        override def run() {
            loop
        }
    }

    @volatile private var prevTime = -1L
    @volatile private var nextTime = -1L
    @volatile private var stopped = false

    /**
      * Get the time when this timer will fire if it is started right now.
      * The time will be a multiple of this timer's period and more than
      * current system time.
      */
    def getStartTime(): Long = {
        (math.floor(clock.getTimeMillis().toDouble / period) + 1).toLong * period
    }

    /**
      * Get the time when the timer will fire if it is restarted right now.
      * This time depends on when the timer was started the first time, and was stopped
      * for whatever reason. The time must be a multiple of this timer's period and
      * more than current time.
      */
    def getRestartTime(originalStartTime: Long): Long = {
        val gap = clock.getTimeMillis() - originalStartTime
        (math.floor(gap.toDouble / period).toLong + 1) * period + originalStartTime
    }

    /**
      * Start at the given start time.
      */
    def start(startTime: Long): Long = synchronized {
        nextTime = startTime
        thread.start()
        logInfo("Started timer for " + name + " at time " + nextTime)
        nextTime
    }

    /**
      * Start at the earliest time it can start based on the period.
      */
    def start(): Long = {
        start(getStartTime())
    }

    /**
      * Stop the timer, and return the last time the callback was made.
      *
      * @param interruptTimer True will interrupt the callback if it is in progress (not guaranteed to
      *                       give correct time in this case). False guarantees that there will be at
      *                       least one callback after `stop` has been called.
      */
    def stop(interruptTimer: Boolean): Long = synchronized {
        if (!stopped) {
            stopped = true
            if (interruptTimer) {
                thread.interrupt()
            }
            thread.join()
            logInfo("Stopped timer for " + name + " after time " + prevTime)
        }
        prevTime
    }

    private def triggerActionForNextInterval(): Unit = {
        clock.waitTillTime(nextTime)
        callback(nextTime)
        prevTime = nextTime
        nextTime += period
        logDebug("Callback for " + name + " called at time " + prevTime)
    }

    /**
      * Repeatedly call the callback every interval.
      */
    private def loop() {
        try {
            while (!stopped) {
                triggerActionForNextInterval()
            }
            triggerActionForNextInterval()
        } catch {
            case e: InterruptedException =>
        }
    }
}

private[streaming]
object RecurringTimer extends Logging {

    def main(args: Array[String]) {
        var lastRecurTime = 0L
        val period = 1000

        def onRecur(time: Long) {
            val currentTime = System.currentTimeMillis()
            logInfo("" + currentTime + ": " + (currentTime - lastRecurTime))
            lastRecurTime = currentTime
        }
        val timer = new RecurringTimer(new SystemClock(), period, onRecur, "Test")
        timer.start()
        Thread.sleep(30 * 1000)
        timer.stop(true)
    }
}

