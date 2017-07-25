package com.xiaoxiaomo.metrics;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.reporting.ConsoleReporter;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 *
 * 计数器
 *
 * Created by TangXD on 2017/7/25.
 */
public class TestCounter {

    private final Counter counter = Metrics.newCounter(TestCounter.class, "pending-jobs");
    private final static LinkedList<String> queue = new LinkedList<>();


    public void add(String str){
        counter.inc();
        queue.offer(str);
    }

    public String take(){
        counter.dec();
        return queue.poll();
    }

    public static void main(String[] args) throws InterruptedException {

        TestCounter tc = new TestCounter();
        ConsoleReporter.enable(1, TimeUnit.SECONDS);
        while (true){
            tc.add("a");
            Thread.sleep(1000);
        }


    }
}
