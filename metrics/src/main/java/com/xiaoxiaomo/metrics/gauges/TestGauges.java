package com.xiaoxiaomo.metrics.gauges;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

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

    private static final MetricRegistry registry = new MetricRegistry();

    public static Queue<String> queue = new LinkedList<>();

    public static void main(String[] args) throws InterruptedException {

        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(2,TimeUnit.MINUTES);

        Gauge g = registry.register(MetricRegistry.name(TestGauges.class, "requests", "size"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return queue.size();
            }
        }) ;


        queue.add("xiaoxiaomo");
        System.out.println( g.getValue());

        while ( true ){
            Thread.sleep(1000);
            reporter.report();
        }

    }
}
