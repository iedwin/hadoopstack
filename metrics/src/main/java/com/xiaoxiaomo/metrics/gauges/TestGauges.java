package com.xiaoxiaomo.metrics.gauges;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * 仪表
 *
 * 最简单的度量指标（及时值），每次相当于重置这个值。
 *
 * 当你开汽车的时候，当前速度是Gauge值。
 * 你测体温的时候， 体温计的刻度是一个Gauge值。
 * 当你的程序运行的时候， 内存使用量和CPU占用率都可以通过Gauge值来度量。
 *
 * Created by xiaoxiaomo on 2017/7/25.
 */
public class TestGauges {

    private static Queue<String> queue ;
    private static MetricRegistry registry ;
    private static ConsoleReporter reporter ;

    static {
        registry = new MetricRegistry();

        reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(2,TimeUnit.MINUTES);
    }

    public static void main(String[] args) throws InterruptedException {

        Gauge g = getGauge(registry,"job");

        queue = new LinkedList<>();
        queue.add("xiaoxiaomo");

        while ( true ){
            Thread.sleep(1000);
            reporter.report();
        }

    }

    private static Gauge getGauge(MetricRegistry registry , String jobName ) {
        return registry.register(
                MetricRegistry.name(TestGauges.class, jobName, "size"), new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return queue.size();
                    }
                });
    }
}
