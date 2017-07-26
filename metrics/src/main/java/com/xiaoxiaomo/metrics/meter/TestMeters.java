package com.xiaoxiaomo.metrics.meter;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.xiaoxiaomo.metrics.histogram.TestHistograms;

import java.util.concurrent.TimeUnit;

/**
 *
 * Meters会将最近1分钟，5分钟，15分钟的TPS（每秒处理的request数）给打印出来，还有所有时间的TPS。
 *
 * Created by TangXD on 2017/7/25.
 */
public class TestMeters {

    private static final MetricRegistry registry = new MetricRegistry() ;

    private static final Meter meter =
            registry.meter(MetricRegistry.name(TestHistograms.class, "result-counts"));


    public static void main(String[] args) throws InterruptedException {

        //Reporters are the way that your application exports all the measurements being made by its metrics.
        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).build();
        reporter.start(1,TimeUnit.MINUTES);

        while ( true ){
            meter.mark();
            meter.mark();
            Thread.sleep(1000);
            reporter.report();
        }
    }

}
