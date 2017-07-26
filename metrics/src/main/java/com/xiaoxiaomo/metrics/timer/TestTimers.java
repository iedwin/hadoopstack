package com.xiaoxiaomo.metrics.timer;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.xiaoxiaomo.metrics.histogram.TestHistograms;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 *
 * 时间统计
 *
 * Created by xiaoxiaomo on 2017/7/25.
 */
public class TestTimers {

    private static final MetricRegistry registry = new MetricRegistry() ;

    private static final Timer timer =
            registry.timer(MetricRegistry.name(TestHistograms.class, "get-requests"));


    public static void main(String[] args) throws InterruptedException {
//        ConsoleReporter.enable(2,TimeUnit.SECONDS);
        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(2,TimeUnit.MINUTES);

        Random random = new Random();
        timer.time();
        System.out.println();
        while ( true ){
            Timer.Context context = timer.time();
            Thread.sleep(random.nextInt(1000));
            context.stop();
            reporter.report();
        }

    }



}
