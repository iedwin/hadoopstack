package com.xiaoxiaomo.metrics.histogram;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

/**
 *
 * Histograms 直方图
 * 最大值，最小值，平均值，方差，中位值，百分比数据，如75%,90%,98%,99%的数据在哪个范围内
 *
 * Created by TangXD on 2017/7/25.
 */
public class TestHistograms {

    private static final MetricRegistry registry = new MetricRegistry() ;

    private static final Histogram histogram =
            registry.histogram(MetricRegistry.name(TestHistograms.class, "result-counts"));

    public static void main(String[] args) throws InterruptedException {

        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(1,TimeUnit.MINUTES);

        int i = 0 ;
        while ( true ){
            histogram.update(i++);
            Thread.sleep(1000);
            reporter.report();
        }



    }

}
