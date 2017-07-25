package com.xiaoxiaomo.metrics;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.reporting.ConsoleReporter;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 *
 * 最简单的度量指标，每次相当于重置这个值。
 *
 * Created by TangXD on 2017/7/25.
 */
public class TestGauges {

    public static Queue<String> queue = new LinkedList<>();

    public static void main(String[] args) throws InterruptedException {

        ConsoleReporter.enable(5, TimeUnit.SECONDS);

        Gauge<Integer> g = Metrics.newGauge(TestGauges.class, "pending-jobs", new Gauge<Integer>() {
            @Override
            public Integer value() {
                return queue.size();
            }
        });

        queue.add("xiaoxiaomo");
        System.out.println( g.value() );

        while ( true ){
            Thread.sleep(1000);
        }

    }
}
