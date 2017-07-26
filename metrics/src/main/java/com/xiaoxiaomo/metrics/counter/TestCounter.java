package com.xiaoxiaomo.metrics.counter;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 *
 * 计数器
 *
 * Created by xiaoxiaomo on 2017/7/25.
 */
public class TestCounter {

    private static final MetricRegistry registry = new MetricRegistry() ;
    private final static LinkedList<String> queue = new LinkedList<>();
    private static final Counter counter ;

    static {
        counter = registry.counter(MetricRegistry.name(TestCounter.class, "cache-evictions"));
    }

    public static void add(String str){
        counter.inc();
        queue.offer(str);
    }

    public String take(){
        counter.dec();
        return queue.poll();
    }

    public static void main(String[] args) throws InterruptedException {

        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(2,TimeUnit.MINUTES);

        TestCounter tc = new TestCounter();
        while (true){
            tc.add("a");
            Thread.sleep(1000);
            reporter.report();
        }


    }
}
